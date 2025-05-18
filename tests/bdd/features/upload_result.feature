# language: zh-TW
功能: 可以新增result
    場景: 可以新增result from stdout

    cat `result.file` | qqabc upload result --job-id `job_id` --from-stdout

        假設 線上有一個job
        當 我upload result by stdout
        那麼 我可以看到這個result

    場景: 可以新增result from file

    qqabc upload result --job-id `job_id` --from-file `result.file`

        假設 線上有一個job
        當 我upload result by file
        那麼 我可以看到這個result

    場景: 可以新增result from data

    qqabc upload result --job-id `job_id` --data `result string`

        假設 線上有一個job
        當 我upload result by data
        那麼 我可以看到這個result

    場景: 可以下載result

    qqabc download result --job-id `job_id`

    場景: 有兩個result時，預設下載最新的result

    qqabc download result --job-id `job_id`

    場景: 有兩個result時，下載指定的result

    qqabc download result --job-id `job_id`--index 0
    qqabc download result --job-id `job_id`--index 1
