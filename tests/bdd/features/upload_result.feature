# language: zh-TW
功能: 可以新增result
    場景: 可以新增result from stdout

    cat `result.file` | qqabc upload result --job-id `job_id` --from-stdout

        假設 線上有一個job
        當 我upload result by stdout
        而且 下載result
        那麼 我可以看到這個result

    場景: 可以新增result from file

    qqabc upload result --job-id `job_id` --from-file `result.file`

        假設 線上有一個job
        當 我upload result by file
        而且 下載result
        那麼 我可以看到這個result

    場景: 可以新增result from data

    qqabc upload result --job-id `job_id` --data `result string`

        假設 線上有一個job
        當 我upload result by data
        而且 下載result
        那麼 我可以看到這個result

    場景: 預設下載result到cwd

    qqabc download result --job-id `job_id`

        假設 線上有一個job
        而且 有一個result
        當 我下載result(不指定dest)
        那麼 我可以看到這個result

    場景: 可以下載result到stdout

    qqabc download result --job-id `job_id` --to-stdout

        假設 線上有一個job
        而且 有一個result
        當 我下載result to stdout
        那麼 我可以看到這個result

    場景: 可以下載result到file

    qqabc download result --job-id `job_id` --to-file `result.file`

        假設 線上有一個job
        而且 有一個result
        當 我下載result to file
        那麼 我可以看到這個result

    場景: 可以下載result到dir

    qqabc download result --job-id `job_id` --to-dir `dir`

        假設 線上有一個job
        而且 有一個result
        當 我下載result to dir
        那麼 我可以看到這個result

    場景: 有多個result時，預設下載最新的result

    qqabc download result --job-id `job_id`

        假設 線上有一個job
        而且 有多個result
        當 我下載result
        那麼 我可以看到最新的result

    場景: 有多個result時，下載指定的result

    qqabc download result --job-id `job_id`--index 0
    qqabc download result --job-id `job_id`--index 1

        假設 線上有一個job
        而且 有多個result
        當 我下載result指定第二個
        那麼 我可以看到第二個result
