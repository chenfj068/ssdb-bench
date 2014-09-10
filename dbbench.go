package main

import (
	"fmt"
	"github.com/jiecao-fm/ssdb"
	"strconv"
	"sync"
)

type SetConf struct {
	name_prefix string
	set_start   int
	set_end     int
}

func WriteSets(conf SetConf, dbpool *ssdb.SSDBPool, g *sync.WaitGroup) {
	db, _ := dbpool.GetDB()
	defer dbpool.ReturnDB(db)
	for i := conf.set_start; i < conf.set_end; i++ {
		setname := conf.name_prefix + "_" + strconv.Itoa(i)
		for j := 0; j < 300; j++ {

			db.ZIncr(setname, "key_"+strconv.Itoa(j), int64(j))
			if j%100 == 0 {
				fmt.Printf("set [%s] progress to  %d\n", setname, j)
			}
		}
		fmt.Printf("set finished %s\n", setname)
	}

	g.Done()
}

var (
	total_sets    = 3
	routing_count = 2
)

func main() {
	g := &sync.WaitGroup{}

	fmt.Printf("hello bench\n")
	count := total_sets / routing_count
	if(total_sets%routing_count!=0){
		count=count+1
	}
	
	g.Add(routing_count)
	poolconf := ssdb.PoolConfig{Host: "jiecao-tucao", Port: 8888, Initial_conn_count: 3, Max_idle_count: 5, Max_conn_count: 10, CheckOnGet: true}
	dbpool, _ := ssdb.NewPool(poolconf)
	for i := 0; i < routing_count; i++ {
		conf := SetConf{name_prefix: "set_", set_start: i * count}
		set_end := count * (i + 1)
		if set_end > total_sets {
			set_end = total_sets
		}
		conf.set_end = set_end
		go WriteSets(conf, dbpool, g)

	}

	g.Wait()
}
