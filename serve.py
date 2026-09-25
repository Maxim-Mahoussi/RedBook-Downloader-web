from os import environ

from uvicorn import run

if __name__ == "__main__":
    run(
        "web:app",
        host="0.0.0.0",
        port=int(environ.get("PORT", "8000")),
    )
