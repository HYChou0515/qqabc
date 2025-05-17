# language: zh-TW
功能: 可以新增result
    場景: 可以新增result from stdin

    cat `result.file` | qqabc update `job_id` -s `status` --stdin

        假設 線上有一個job
        當 我update result
        那麼 我可以看到這個result

    場景: 可以新增result from file

    qqabc update `job_id` -s `status` --file `result.file`

    場景: 可以新增result from command

    qqabc update `job_id` -s `status` --data `result string`

    場景: 可以下載result

    qqabc get result `job_id`

    場景: 有兩個result時，預設下載最新的result

    qqabc get result `job_id`

    場景: 有兩個result時，下載指定的result

    qqabc get result `job_id` --index 0
    qqabc get result `job_id` --index 1
