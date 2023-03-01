
This utility is very similar to the Linux `time` command, but it saves the results (via sending an Eventkit UDP package, which can be persisted)

You can test it with running:

```
eventkit-time -d eventkitd.datasci.storj.io:9002 sleep 2
```

And on BQ console (change instance and date):

```
SELECT * FROM `storj-data-science-249814.eventkitd2.eventkit_time_test` WHERE DATE(received_at) = "2023-03-01" and source_instance = 'pw' LIMIT 1000
```

Or test it locally:

```
cd ../eventkit-receiver
go run .
```

```
eventkit-time -d localhost:9002 sleep 2
```
