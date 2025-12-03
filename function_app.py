import azure.functions as func
from reed_ingest import main as reed_ingest_main

app = func.FunctionApp()

@app.function_name(name="reed_ingest")
@app.timer_trigger(schedule="0 0 0 * * *", arg_name="mytimer",
                   run_on_startup=False, use_monitor=False)
def reed_ingest(mytimer: func.TimerRequest):
    """V2 wrapper for the reed_ingest V1 function. Runs daily at midnight UTC."""
    reed_ingest_main(mytimer)