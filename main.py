import os
import re
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters, CallbackQueryHandler
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, Session, sessionmaker

from searchdialog import search_handler, handle_text_response, handle_callback_query

from userauth import get_user_email, set_user_email, is_valid_email

# Load environment
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")


# Command /start
async def command_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = str(update.effective_user.id)
    email = get_user_email(user_id)
    email_status = email if email else "Not set"
    print(f"User {user_id} started the bot. Email: {email_status}")
    message = (
        "ℹ️ Info center\n"
        f"Email status: {'✅' if email else '❌'}\n"
        f"{email_status}\n\n"
        "/setemail - set new or update current email\n\n"
        "Main commands:\n/search - main function"
    )
    await update.message.reply_text(message)

# Command /setemail
async def command_setemail(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Please enter your email address:")
    context.user_data["awaiting_email"] = True



# Init bot
def main():
    app = Application.builder().token(BOT_TOKEN).build()

    # Command handlers
    app.add_handler(CommandHandler("start", command_start))
    app.add_handler(CommandHandler("setemail", command_setemail))
    app.add_handler(CommandHandler("search", search_handler))  

    # Text messages
    #app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text_response))
    app.add_handler(CallbackQueryHandler(handle_callback_query))


    print("Bot is running...")
    app.run_polling()

if __name__ == "__main__":
    main()
