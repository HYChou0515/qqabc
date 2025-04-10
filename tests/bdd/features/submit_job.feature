# language: zh-TW
功能: 可以送出job
    場景: 送出一個job

    jobcli submit job --file xxx.json

        假設 我有一個job
        當 我送出這個job
        那麼 我可以在job list裡面看到這個job

    場景: 用stdin送出一個job

    jobcli submit job --file xxx.json

        假設 我有一個job
        當 我用stdin送出這個job
        那麼 我可以在job list裡面看到這個job
