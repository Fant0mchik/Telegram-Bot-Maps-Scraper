from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from parser import run_collector_in_thread, create_google_sheet, LOCATIONS
from userauth import get_user_email, set_user_email, is_valid_email
STATES_PER_PAGE = 10
STATE_CODES = sorted(LOCATIONS.keys())

def get_state_keyboard(page: int = 0):
    start = page * STATES_PER_PAGE
    end = start + STATES_PER_PAGE
    page_states = STATE_CODES[start:end]

    buttons = [[InlineKeyboardButton(state, callback_data=f"state:{state}")] for state in page_states]

    navigation = []
    if page > 0:
        navigation.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"page:{page - 1}"))
    if end < len(STATE_CODES):
        navigation.append(InlineKeyboardButton("Next ➡️", callback_data=f"page:{page + 1}"))

    if navigation:
        buttons.append(navigation)

    return InlineKeyboardMarkup(buttons)

async def search_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 Please enter a keyword for search:")
    context.user_data["search_stage"] = "awaiting_keyword"

async def handle_text_response(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stage = context.user_data.get("search_stage")
    if context.user_data.get("awaiting_email"):
        email = update.message.text.strip()
        if is_valid_email(email):
            user_id = str(update.effective_user.id)
            set_user_email(user_id, email)
            context.user_data["awaiting_email"] = False
            await update.message.reply_text(f"✅ Email saved: {email}")
        else:
            await update.message.reply_text("❌ Invalid email. Try again:")
    elif stage == "awaiting_keyword":
        keyword = update.message.text.strip()
        if not keyword:
            await update.message.reply_text("❌ Please provide a valid keyword.")
            return
        context.user_data["keyword"] = keyword
        context.user_data["search_stage"] = "awaiting_state"
        await update.message.reply_text("🌎 Please select a US state:", reply_markup=get_state_keyboard(0))
    else:
        await update.message.reply_text("Unknown input. Use /search to begin.")

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data.startswith("page:"):
        page = int(data.split(":")[1])
        await query.edit_message_reply_markup(reply_markup=get_state_keyboard(page))
        return

    if data.startswith("state:"):
        state = data.split(":")[1]
        keyword = context.user_data.get("keyword")
        user_id = str(update.effective_user.id)
        email = get_user_email(user_id)

        if not keyword or not email:
            await query.edit_message_text("❌ Session expired or email not set. Use /search to restart.")
            return

        await query.edit_message_text(f"🔁 Starting collection for keyword: `{keyword}` in `{state}`...", parse_mode="Markdown")

        try:
            run_collector_in_thread(keyword, state)
            sheet_url = create_google_sheet(f"'{keyword}' Export in '{state}'", email, keyword, state)
            await query.message.reply_text(f"✅ Your Google Sheet:\n{sheet_url}")
        except Exception as e:
            await query.message.reply_text(f"❌ Error occurred: {str(e)}")
