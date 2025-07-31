class Args:
    def __init__(self):
        self.job_name:str = ''
        self.list_columns = False
    
    def __str__(self):
        return f"Args(JobName= {self.job_name}, ListColumns= {self.list_columns})"

