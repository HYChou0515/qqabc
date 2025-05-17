# language: zh-TW
功能: 可以新增status
    場景: 可以新增一個status

    qqabc update `job_id` -s `status`
    qqabc get status `job_id`

        假設 線上有一個job
        當 我update status
        那麼 我可以看到這個status
    
    場景: 可以新增第多個status
        假設 線上有一個job
        當 我update status多次
        那麼 我可以看到最新的status

    場景: 新增status可以帶上detail

    qqabc update `job_id` -s `status` -d `detail`
    qqabc get status `job_id`

        假設 線上有一個job
        當 update status with detail
        那麼 我可以看到這個status
