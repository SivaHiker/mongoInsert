package main

import (
	"gopkg.in/mgo.v2"
	"fmt"
	"os"
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
)

func main() {

	//filepath := flag.String("filePath","/Users/siva/Desktop/data05.txt","Fully Qualified Path of the file where " +
	//	"userdata presents")
	//flag.Parse()

	var counter int
	session, err := mgo.Dial("10.15.0.149")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	c := session.DB("userlist").C("mqttuserdata")
	fmt.Println(c.Name)

	files, err := ioutil.ReadDir("/Users/siva/Desktop/sivalocal")
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		fmt.Println("Starting parsing for the file -->", f.Name())
		file, err := os.Open("/Users/siva/Desktop/sivalocal/"+f.Name())
		defer file.Close()

		if err != nil {
			println(err)
		}
		reader := bufio.NewReader(file)
		for {
			var buffer bytes.Buffer
			var line []byte
			line, _, err = reader.ReadLine()
			buffer.Write(line)
			println(buffer.String())
			// If we're just at the EOF, break
			if err != nil {
				break
			} else {
				var user UserData
				json.Unmarshal([]byte(line), &user)
				userinsert := UserInfo{UserData: user, Flag: false}
				fmt.Println(userinsert.UserData)
				fmt.Println(userinsert.UserData.Token)
				fmt.Println(userinsert.Flag)
				c.Insert(userinsert)
				counter++
			}
		}

		if err != io.EOF {
			fmt.Printf(" > Failed!: %v\n", err)
		}

		fmt.Println("Total Inserted Document Count ",counter)
	}

}



type UserInfo struct {
	UserData UserData `json:"UserData"`
	Flag bool `json:"flag"`
}

type UserData struct {
	Msisdn string `json:"msisdn"`
	Token  string `json:"token"`
	UID    string `json:"uid"`
}