package main

import (
    "encoding/csv"
    "labix.org/v2/mgo"
    "labix.org/v2/mgo/bson"
    "os"
    "fmt"
    "io"
)

func main() {

    file, err := os.Open(os.Args[1])
    if err != nil {
        return
    }
    defer file.Close()
    reader := csv.NewReader(file)
    mongo(reader)
}

func mongo(reader *csv.Reader) {
    //set up the connection to mongodb.
    session, err := mgo.Dial("localhost")
    if err != nil {
        panic(err)
    }
    defer session.Close()
    // Optional. Switch the session to a monotonic behavior.
    session.SetMode(mgo.Monotonic, true)

    //create the database
    c := session.DB("test").C("people")

    //Build the columns.
    var fields []string
    firstFieldRead := 0
    for {
        //Read th file.
        record, err := reader.Read()
        if err == io.EOF {
            break
        } else if err != nil {
            fmt.Println("Error:", err)
            return
        }
        //build the table names.
        if firstFieldRead == 0 {
            fields = record
            firstFieldRead = 1
        } else {
            data := make(bson.M)
            for i := 0; i < len(fields); i++ {
                key := fields[i]
                data[key] = record[i]
            }
            err = c.Insert(data);
            if err != nil {
                panic(err)
            }
        }
    }
}
