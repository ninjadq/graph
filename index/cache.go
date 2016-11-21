package index

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	cmodel "github.com/open-falcon/common/model"
	cutils "github.com/open-falcon/common/utils"
	"github.com/open-falcon/graph/g"
	"github.com/open-falcon/graph/proc"
	tcache "github.com/toolkits/cache/localcache/timedcache"
)

const (
	DefaultMaxCacheSize                     = 5000000 // 默认 最多500w个,太大了内存会耗尽
	DefaultCacheProcUpdateTaskSleepInterval = time.Duration(1) * time.Second
)

// item缓存
var (
	indexedItemCache   = NewIndexCacheBase(DefaultMaxCacheSize)
	unIndexedItemCache = NewIndexCacheBase(DefaultMaxCacheSize)
	monitorItemCache   = NewMonitorItemCache()
)

// db本地缓存
var (
	// endpoint表的内存缓存, key:endpoint(string) / value:id(int64)
	dbEndpointCache = tcache.New(600*time.Second, 60*time.Second)
	// endpoint_counter表的内存缓存, key:endpoint_id-counter(string) / val:dstype-step(string)
	dbEndpointCounterCache = tcache.New(600*time.Second, 60*time.Second)
)

// 初始化cache
func InitCache() {
	monitorItemCache.InitItemCache()
	go startCacheProcUpdateTask()
}

// USED WHEN QUERY
func GetTypeAndStep(endpoint string, counter string) (dsType string, step int, found bool) {
	// get it from index cache
	pk := cutils.Md5(fmt.Sprintf("%s/%s", endpoint, counter))
	if icitem := indexedItemCache.Get(pk); icitem != nil {
		if item := icitem.(*IndexCacheItem).Item; item != nil {
			dsType = item.DsType
			step = item.Step
			found = true
			return
		}
	}

	// statistics
	proc.GraphLoadDbCnt.Incr()

	// get it from db, this should rarely happen
	var endpointId int64 = -1
	if endpointId, found = GetEndpointFromCache(endpoint); found {
		if dsType, step, found = GetCounterFromCache(endpointId, counter); found {
			//found = true
			return
		}
	}

	// do not find it, this must be a bad request
	found = false
	return
}

// Return EndpointId if Found
func GetEndpointFromCache(endpoint string) (int64, bool) {
	// get from cache
	endpointId, found := dbEndpointCache.Get(endpoint)
	if found {
		return endpointId.(int64), true
	}

	// get from db
	var id int64 = -1
	err := g.DB.QueryRow("SELECT id FROM endpoint WHERE endpoint = ?", endpoint).Scan(&id)
	if err != nil && err != sql.ErrNoRows {
		log.Println("query endpoint id fail,", err)
		return -1, false
	}

	if err == sql.ErrNoRows || id < 0 {
		return -1, false
	}

	// update cache
	dbEndpointCache.Set(endpoint, id, 0)

	return id, true
}

// Return DsType Step if Found
func GetCounterFromCache(endpointId int64, counter string) (dsType string, step int, found bool) {
	var err error
	// get from cache
	key := fmt.Sprintf("%d-%s", endpointId, counter)
	dsTypeStep, found := dbEndpointCounterCache.Get(key)
	if found {
		arr := strings.Split(dsTypeStep.(string), "_")
		step, err = strconv.Atoi(arr[1])
		if err != nil {
			found = false
			return
		}
		dsType = arr[0]
		return
	}

	// get from db
	err = g.DB.QueryRow("SELECT type, step FROM endpoint_counter WHERE endpoint_id = ? and counter = ?",
		endpointId, counter).Scan(&dsType, &step)
	if err != nil && err != sql.ErrNoRows {
		log.Println("query type and step fail", err)
		return
	}

	if err == sql.ErrNoRows {
		return
	}

	// update cache
	dbEndpointCounterCache.Set(key, fmt.Sprintf("%s_%d", dsType, step), 0)

	found = true
	return
}

// 更新 cache的统计信息
func startCacheProcUpdateTask() {
	for {
		time.Sleep(DefaultCacheProcUpdateTaskSleepInterval)
		proc.IndexedItemCacheCnt.SetCnt(int64(indexedItemCache.Size()))
		proc.UnIndexedItemCacheCnt.SetCnt(int64(unIndexedItemCache.Size()))
		proc.EndpointCacheCnt.SetCnt(int64(dbEndpointCache.Size()))
		proc.CounterCacheCnt.SetCnt(int64(dbEndpointCounterCache.Size()))
	}
}

// INDEX CACHE
// 索引缓存的元素数据结构
type IndexCacheItem struct {
	UUID string
	Item *cmodel.GraphItem
}

func NewIndexCacheItem(uuid string, item *cmodel.GraphItem) *IndexCacheItem {
	return &IndexCacheItem{UUID: uuid, Item: item}
}

// 索引缓存-基本缓存容器
type IndexCacheBase struct {
	sync.RWMutex
	maxSize int
	data    map[string]interface{}
}

func NewIndexCacheBase(max int) *IndexCacheBase {
	return &IndexCacheBase{maxSize: max, data: make(map[string]interface{})}
}

func (this *IndexCacheBase) GetMaxSize() int {
	return this.maxSize
}

func (this *IndexCacheBase) Put(key string, item interface{}) {
	this.Lock()
	defer this.Unlock()
	this.data[key] = item
}

func (this *IndexCacheBase) Remove(key string) {
	this.Lock()
	defer this.Unlock()
	delete(this.data, key)
}

func (this *IndexCacheBase) Get(key string) interface{} {
	this.RLock()
	defer this.RUnlock()
	return this.data[key]
}

func (this *IndexCacheBase) ContainsKey(key string) bool {
	this.RLock()
	defer this.RUnlock()
	return this.data[key] != nil
}

func (this *IndexCacheBase) Size() int {
	this.RLock()
	defer this.RUnlock()
	return len(this.data)
}

func (this *IndexCacheBase) Keys() []string {
	this.RLock()
	defer this.RUnlock()

	count := len(this.data)
	if count == 0 {
		return []string{}
	}

	keys := make([]string, 0, count)
	for key := range this.data {
		keys = append(keys, key)
	}

	return keys
}

type MonitorItemCache struct {
	synced   map[string]bool
	unsynced map[string]map[string]string
	sync.RWMutex
}

// initItemCache init the MonitorItemCaches
func (m *MonitorItemCache) InitItemCache() {
	m.Lock()
	defer m.Unlock()
	log.Println("[INFO] Init MonitorItem Cache")
	sqlStr := `SELECT metric, tag.name, value from monitorconfig_item  as item
LEFT JOIN monitorconfig_item_tags as it  ON item.id=it.item_id
LEFT JOIN monitorconfig_tag as tag ON tag.id=it.tag_id
LEFT JOIN monitorconfig_tag_options as top ON top.tag_id = tag.id
LEFT JOIN monitorconfig_option as o ON o.id = top.option_id`
	rows, err := g.AutoDB.Query(sqlStr)
	if err != nil {
		log.Fatalln("[Error] init MonitorItemCache failure", err)
	}
	defer rows.Close()
	for rows.Next() {
		var metric, tag, value sql.NullString
		if err := rows.Scan(&metric, &tag, &value); err != nil {
			log.Fatal(err)
		}
		key := ""
		var buffer bytes.Buffer
		if metric.Valid {
			buffer.WriteString(metric.String)
		}
		if tag.Valid && value.Valid {
			buffer.WriteString(tag.String)
			buffer.WriteString(value.String)
		}
		key = buffer.String()
		if key == "" {
			continue
		}
		m.synced[key] = true
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}

// insertEndpoint将
func (m *MonitorItemCache) insertItemCacheIfNotExist(metric, tag, value, dstype string) {
	key := metric + tag + value
	m.Lock()
	defer m.Unlock()
	if _, ok := m.synced[key]; !ok {
		if _, ok := m.unsynced[key]; !ok {
			log.Println("[INFO] A New Item is coming", metric, tag, value)
			m.unsynced[key] = map[string]string{
				"metric": metric,
				"tag":    tag,
				"value":  value,
				"dstype": dstype,
			}
		}
	}
}

// syncItemCache sync unsynced Item to DB
func (m *MonitorItemCache) syncItemCache() {
	m.Lock()
	defer m.Unlock()
	queryItemSql := "SELECT id FROM monitorconfig_item WHERE metric = ?"
	insertItemSql := "INSERT INTO monitorconfig_item (name, metric, value_type, has_tag, has_log) VALUES( ?, ?, ?, 0, 0)"
	queryItemTagSql := "SELECT tag.id From monitorconfig_item_tags AS it LEFT JOIN monitorconfig_tag as tag ON tag.id=it.tag_id WHERE it.item_id = ( ? ) AND tag.name = ( ? )"
	insertTagSql := "INSERT INTO monitorconfig_tag (name, `explain`) VALUES( ?, ? )"
	insertItemTag := "INSERT INTO monitorconfig_item_tags (item_id, tag_id) VALUES(? , ?)"
	queryTagOptSql := "SELECT o.id FROM monitorconfig_tag_options AS top LEFT JOIN monitorconfig_option as o ON top.option_id = o.id WHERE top.tag_id= ( ? ) AND o.value= ( ? )"
	insertOptSql := "INSERT INTO monitorconfig_option (value) VALUES ( ? )"
	insertTagOptsSql := "INSERT INTO monitorconfig_tag_options (tag_id, option_id) VALUES ( ? , ? )"

	var item_id, tag_id, opt_id int64
	var metric, tag, option, dstype string
	var result sql.Result
	var err error
	var tx *sql.Tx

	for k, v := range m.unsynced {
		metric, tag, option, dstype = v["metric"], v["tag"], v["value"], v["dstype"]
		if g.Config().Debug {
			log.Println("[DEBUG] start sync...", metric, tag, option, dstype)
		}

		tx, err = g.AutoDB.Begin()
		if err != nil {
			log.Println("[ERROR] when start a transaction", k, err)
			continue
		}
		// 查找item的id，不存在就插入
		err = tx.QueryRow(queryItemSql, metric).Scan(&item_id)
		if err != nil { //DB中查询Item出错
			if err == sql.ErrNoRows { // 错误为 item 不存在,则插入
				result, err := tx.Exec(insertItemSql, metric, metric, dstype)
				if err != nil { // 插入item出错，回滚
					log.Println("[ERROR] Insert metric failure", k, err)
					tx.Rollback()
					continue
				}
				item_id, _ = result.LastInsertId()
			} else { // 其他错误错,回滚
				log.Println("[DEBUG] when query item ", k, err)
				tx.Rollback()
				continue
			}
		}

		//tag为空就直接commit
		if tag == "" {
			if err = tx.Commit(); err != nil {
				log.Println("[ERROR] when commit tx", k, err)
			} else {
				m.synced[k] = true
				delete(m.unsynced, k)
				if g.Config().Debug {
					log.Println("[DEBUG] insert success only metrci", metric)
				}
			}
			continue
		}
		err = tx.QueryRow(queryItemTagSql, item_id, tag).Scan(&tag_id)
		if err != nil { //查找tag出错
			if err == sql.ErrNoRows { // tag 不存在则插入
				result, err = tx.Exec(insertTagSql, tag, tag)
				if err != nil { //插入失败，回滚
					log.Println("[ERROR] when Insert tag", k, err)
					tx.Rollback()
					continue
				}
				tag_id, _ = result.LastInsertId()
			} else { //查DB出错，回滚
				log.Println("[ERROR] when query tag", k, err)
				tx.Rollback()
				continue
			}

		}
		_, err = tx.Exec(insertItemTag, item_id, tag_id)
		if err != nil {
			if !strings.Contains(err.Error(), "Duplicate entry") {
				tx.Rollback()
				log.Println("[ERROR] when insert Item tags", k, v, err)
				continue
			}
		}

		//查找option如果没有则回滚，因为这是错误的行为不能有没值的tag
		if option == "" {
			log.Println("[ERROR] tag is not nil but option is nil", tag, option)
			tx.Rollback()
			continue
		}
		err = tx.QueryRow(queryTagOptSql, tag_id, option).Scan(&opt_id)
		if err != nil { // 如果是没有option则插入
			if err == sql.ErrNoRows {
				result, err := tx.Exec(insertOptSql, option)
				if err != nil { // 插入失败回滚
					log.Println("[ERROR] when insert option", k, err)
					tx.Rollback()
					continue
				}
				opt_id, _ = result.LastInsertId()
			} else { // 查询出错也回滚
				log.Println("[ERROR] when query option", k, err)
				tx.Commit()
				continue
			}
		}
		_, err = tx.Exec(insertTagOptsSql, tag_id, opt_id)
		if err != nil {
			if !strings.Contains(err.Error(), "Duplicate entry") {
				log.Println("[ERROR] when insert tag options", k, v)
				tx.Rollback()
				continue
			}
		}

		if err = tx.Commit(); err != nil {
			log.Println("[ERROR] when commit tx", k, err)
			continue
		} else {
			m.synced[k] = true
			delete(m.unsynced, k)
			log.Println("[INFO] insert success", metric, tag, option)
		}

	}
}

func NewMonitorItemCache() *MonitorItemCache {
	return &MonitorItemCache{synced: make(map[string]bool),
		unsynced: make(map[string]map[string]string)}
}

// 周期性的同步MonitorCahce到monitorsystem的数据库中
func SyncItem2DBTask() {
	tc := time.Tick(time.Minute)
	for range tc {
		monitorItemCache.syncItemCache()
	}
}

func InsertMonitorCacheIfNeed(item *cmodel.GraphItem) {
	if len(item.Tags) == 0 {
		monitorItemCache.insertItemCacheIfNotExist(item.Metric, "", "", item.DsType)
	} else {

		for tag, value := range item.Tags {
			monitorItemCache.insertItemCacheIfNotExist(item.Metric, tag, value, item.DsType)
		}
	}
}
