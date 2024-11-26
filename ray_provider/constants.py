from ray.job_submission import JobStatus


TERMINAL_JOB_STATUSES = {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}