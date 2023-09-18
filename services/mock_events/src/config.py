class Config:
    def __init__(self) -> None:
        self.RedpandaTopic = "events_processed"
        self.RedpandaBrokerHost = "localhost:19092"

    def __str__(self) -> str:
        result = f"RedpandaTopic:{self.RedpandaTopic}"
        result += f"RedpandaBrokerHost:{self.RedpandaBrokerHost}"
        return result
