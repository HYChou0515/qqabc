# language: zh-TW
功能: 可以consume job
    場景: 可以吃job
        假設 線上有一個job
        當 我consume job
        那麼 我可以取得這個job
        而且 這個job不再存在於job list裡面
    
    場景: 當job list裡面沒有job
        假設 job list裡面沒有job
        當 我consume job
        那麼 我不會取得job

    場景: 當job list裡面有多個job
        假設 job list裡面有多個job
        當 我consume job
        那麼 我可以取得priority最高的job
        而且 這個job不再存在於job list裡面

