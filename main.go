// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/withlin/canal-go/client"
	pbe "github.com/withlin/canal-go/protocol/entry"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)
var db *gorm.DB
var err error
func init() {

	dsn := "root:123456@tcp(127.0.0.1:3306)/master?charset=utf8mb4&parseTime=True&loc=Local"
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("error",err)
	}

}

type Member struct {
	Id int64
	Name string
}
func(*Member) TableName() string {
	return "member"
}
func main() {
	//sli := []interface{}{"a","b"}
	//sliceToStr(sli)
	//return
	// 192.168.199.17 替换成你的canal server的地址
	// example 替换成-e canal.destinations=example 你自己定义的名字
	connector := client.NewSimpleCanalConnector("192.168.215.131", 11111, "", "", "example", 60000, 60*60*1000)
	err := connector.Connect()
	if err != nil {
		log.Println("xxx:",err)
		os.Exit(1)
	}

	// https://github.com/alibaba/canal/wiki/AdminGuide
	//mysql 数据解析关注的表，Perl正则表达式.
	//
	//多个正则之间以逗号(,)分隔，转义符需要双斜杠(\\)
	//
	//常见例子：
	//
	//  1.  所有表：.*   or  .*\\..*
	//	2.  canal schema下所有表： canal\\..*
	//	3.  canal下的以canal打头的表：canal\\.canal.*
	//	4.  canal schema下的一张表：canal\\.test1
	//  5.  多个规则组合使用：canal\\..*,mysql.test1,mysql.test2 (逗号分隔)

	//err = connector.Subscribe(".*\\..*")
	err = connector.Subscribe("master\\..*")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	for {

		message, err := connector.Get(100, nil, nil)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
		batchId := message.Id
		if batchId == -1 || len(message.Entries) <= 0 {
			time.Sleep(300 * time.Millisecond)
			fmt.Println("===没有数据了===")
			continue
		}

		printEntry(message.Entries)

	}
}

func printEntry(entrys []pbe.Entry) {

	for _, entry := range entrys {
		if entry.GetEntryType() == pbe.EntryType_TRANSACTIONBEGIN || entry.GetEntryType() == pbe.EntryType_TRANSACTIONEND {
			continue
		}
		rowChange := new(pbe.RowChange)

		err := proto.Unmarshal(entry.GetStoreValue(), rowChange)
		checkError(err)
		if rowChange != nil {
			//eventType := rowChange.GetEventType()
			header := entry.GetHeader()
			fmt.Println(fmt.Sprintf("================> binlog[%s : %d],name[%s,%s], eventType: %s", header.GetLogfileName(), header.GetLogfileOffset(), header.GetSchemaName(), header.GetTableName(), header.GetEventType()))
			//fmt.Println("sql:",rowChange.GetSql())
			//fmt.Println(rowChange.GetRowDatas())

			eventType := header.GetEventType()
			for _, rowData := range rowChange.GetRowDatas() {
				if eventType == pbe.EventType_INSERT {
					insertSql(rowData,header)
				}else if eventType == pbe.EventType_DELETE {
					deleteSql(rowData,header)
				}else {
					updateSql(rowData,header)
				}

			}
		}
	}
}
// 获取key
func getKeys(m map[string]interface{}) []interface{} {
	keys := make([]interface{},0,len(m))
	for key := range m {
		keys = append(keys,key)
	}
	return keys
}
// h获取map 的value => []
func getValues(m map[string]interface{}) []interface{} {
	vals := make([]interface{},0,len(m))
	for _,val := range m {

		ss,ok := val.(string)
		fmt.Println(ok,ss)
		vals = append(vals,"'" + ss + "'")

	}
	return vals
}
// 切片 拼接成 ， 分割的字符串
func sliceToStr(sli []interface{}) string {
	str := strings.Replace(strings.Trim(fmt.Sprint(sli),"[]")," ",",",-1)
	return str
}
// 新增 sql
func insertSql(rowData *pbe.RowData,header *pbe.Header) {
	fields := make(map[string]interface{})
	var str strings.Builder
	// instert into member (id,name) values (1,"xxx");
	str.WriteString("insert into ")
	str.WriteString(header.GetSchemaName() + "." + header.GetTableName())
	str.WriteString(" (")

	for _,col := range rowData.GetAfterColumns() {
		fields[col.GetName()] = col.GetValue()
	}
	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	keys := getKeys(fields)
	keysStr := sliceToStr(keys)
	fmt.Println("ky:",keysStr)
	str.WriteString(keysStr)
	str.WriteString(") ")
	str.WriteString("values (")
	vals := getValues(fields)
	valsStr := sliceToStr(vals)
	fmt.Println(valsStr)
	str.WriteString(valsStr)
	str.WriteString(")")
	sql := str.String()
	fmt.Println("sql:",sql)
	db.Exec(sql)
}
func deleteSql(rowData *pbe.RowData,header *pbe.Header) {
	// delete from member where id=1
	var str strings.Builder
	str.WriteString("delete from ")
	str.WriteString(header.GetSchemaName() + "." + header.GetTableName())
	str.WriteString(" where id=")
	var id string
	for _,col := range rowData.GetBeforeColumns() {
		fmt.Println(col.GetName(),col.GetValue())
		if col.GetName() == "id" {
			id = col.GetValue()
			break
		}
	}
	str.WriteString(id)
	sql := str.String()
	fmt.Println(sql)
	db.Exec(sql)
}
// 修改 sql 语句
func updateSql(rowData *pbe.RowData,header *pbe.Header) {
	var str strings.Builder
	result,key,val := getUpdateCols(rowData)
	if len(result) > 0 {
		str.WriteString("update ")
		str.WriteString(header.GetSchemaName() + "." + header.GetTableName())
		str.WriteString(" set ")
		i := 0
		for kk,vv := range result {
			str.WriteString(kk)
			str.WriteString("=")
			str.WriteString("'" + vv + "'")
			i++
			if i != len(result) {
				str.WriteString(",")
			}
		}

		str.WriteString(" where ")
		str.WriteString(key)
		str.WriteString("=")
		str.WriteString(val)
		sql := str.String()
		fmt.Println("sql:",sql)
		db.Exec(sql)
	}

}
// result 获取更改的字段
// key    主键名
// val	  主键值
func getUpdateCols(rowData *pbe.RowData) (result map[string]string,key string,val string) {
	result = make(map[string]string)
	for _,col := range rowData.GetAfterColumns() {
		if col.GetUpdated() {
			result[col.GetName()] = col.GetValue()
		}
		if col.GetIsKey() {
			key = col.GetName()
			val = col.GetValue()
		}
	}
	return
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
