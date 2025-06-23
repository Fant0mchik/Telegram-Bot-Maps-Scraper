
import os
from dotenv import load_dotenv
from telegram.ext import Application, CommandHandler
from parser import start_handler

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")

def main():
    app = Application.builder().token(BOT_TOKEN).build()

    # Додаємо хендлер
    app.add_handler(CommandHandler("start", start_handler))

    print("Bot is running...")
    app.run_polling()

if __name__ == "__main__":
    main()
