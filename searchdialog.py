from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from parser import run_collector_in_thread, create_google_sheet, LOCATIONS, wait_for_task
from userauth import get_user_email, set_user_email, is_valid_email
from db import SessionLocal, User
from worldcities import filter_cities_by_state_and_population
import os
STATES_PER_PAGE = 10
STATE_CODES = sorted(LOCATIONS.keys())
CITY_TYPES = {
    "large": 500000,
    "medium": 250000,
    "small": 100000,
}

def get_state_keyboard(page: int = 0):
    start = page * STATES_PER_PAGE
    end = start + STATES_PER_PAGE
    page_states = STATE_CODES[start:end]

    buttons = [[InlineKeyboardButton(state, callback_data=f"state:{state}")] for state in page_states]
    
    buttons.append([InlineKeyboardButton("🌎 Select All States", callback_data="state:ALL")])

    navigation = []
    if page > 0:
        navigation.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"page:{page - 1}"))
    if end < len(STATE_CODES):
        navigation.append(InlineKeyboardButton("Next ➡️", callback_data=f"page:{page + 1}"))

    if navigation:
        buttons.append(navigation)

    return InlineKeyboardMarkup(buttons)

def get_city_type_keyboard():
    buttons = [
        [InlineKeyboardButton("🏙️ Large Cities (>= 500,000 pop)", callback_data="city_type:large")],
        [InlineKeyboardButton("🏘️ Medium Cities (>= 250,000 pop)", callback_data="city_type:medium")],
        [InlineKeyboardButton("🏡 Small Cities (>= 100,000 pop)", callback_data="city_type:small")],
        [InlineKeyboardButton("🌆 All City Types (Enter pop manualy)", callback_data="city_type:all")],
        [InlineKeyboardButton("🔍 Enter City Manually", callback_data="city_type:manual")]
    ]
    return InlineKeyboardMarkup(buttons)

async def search_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 Please enter a keyword for search:")
    context.user_data["search_stage"] = "awaiting_keyword"
    context.user_data["search_data"] = {}

async def handle_text_response(update: Update, context: ContextTypes.DEFAULT_TYPE):
    username = update.effective_user.username or "Unknown"
    stage = context.user_data.get("search_stage")
    search_data = context.user_data.get("search_data", {})
    
    if context.user_data.get("awaiting_email"):
        email = update.message.text.strip()
        if is_valid_email(email):
            user_id = str(update.effective_user.id)
            set_user_email(user_id, email, username)
            context.user_data["awaiting_email"] = False
            await update.message.reply_text(f"✅ Email saved: {email}")
        else:
            await update.message.reply_text("❌ Invalid email. Try again:")
    
    elif stage == "awaiting_keyword":
        keyword = update.message.text.strip()
        if not keyword:
            await update.message.reply_text("❌ Please provide a valid keyword.")
            return
        search_data["keyword"] = keyword
        context.user_data["search_stage"] = "awaiting_state"
        await update.message.reply_text("🌎 Please select a US state:", reply_markup=get_state_keyboard(0))
    
    elif stage == "awaiting_city_name":
        city_name = update.message.text.strip()
        if not city_name:
            await update.message.reply_text("❌ Please provide a valid city name.")
            return
        search_data["city_name"] = city_name
        search_data["city_type"] = "manual"
        await execute_search(update, context, search_data)
    elif stage == "awaiting_population":
        try:
            min_population = int(update.message.text.strip())
            if min_population <= 0:
                raise ValueError("Population must be a positive integer.")
            search_data["min_population"] = min_population
            search_data["city_type"] = "all"
            await execute_search(update, context, search_data)
        except ValueError:
            await update.message.reply_text("❌ Invalid population. Please enter a positive integer.")
    else:
        await update.message.reply_text("Unknown input. Use /search to begin.")

def get_reply_target(update):
    if hasattr(update, "message") and update.message:
        return update.message
    elif hasattr(update, "callback_query") and update.callback_query:
        return update.callback_query.message
    return None

async def execute_search(update: Update, context: ContextTypes.DEFAULT_TYPE, search_data: dict):
    keyword = search_data.get("keyword")
    state = search_data.get("state")
    city_type = search_data.get("city_type")
    city_name = search_data.get("city_name")
    min_population = search_data.get("min_population")
    
    user_id = str(update.effective_user.id)
    email = get_user_email(user_id)
    
    reply_target = get_reply_target(update)
    if not keyword or not email:
        if reply_target:
            await reply_target.reply_text("❌ Session expired or email not set. Use /start to restart.")
        return

    filename = filter_cities_by_state_and_population(LOCATIONS[state]["Name"], min_population)
    with open(os.path.join("logs", filename), "r", encoding="utf-8") as f:
                lines = f.readlines()[1:]
                resnum = len(lines)
    search_data["filename"] = filename
    await ask_continue_search(update, context, results=resnum)
    #continue on handle_continue_search
    
async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    search_data = context.user_data.get("search_data", {})

    if data.startswith("page:"):
        page = int(data.split(":")[1])
        await query.edit_message_reply_markup(reply_markup=get_state_keyboard(page))
        return
    
    if data.startswith("state:"):
        state = data.split(":")[1]
        search_data["state"] = state
        context.user_data["search_stage"] = "awaiting_city_type"
        await query.edit_message_text("🏙️ Please select city type:", reply_markup=get_city_type_keyboard())
        return
    
    if data.startswith("city_type:"):
        city_type = data.split(":")[1]
        search_data["city_type"] = city_type
        
        if city_type == "manual":
            context.user_data["search_stage"] = "awaiting_city_name"
            await query.edit_message_text("✏️ Please enter the city name:")
        elif city_type == "all":
            context.user_data["search_stage"] = "awaiting_population"
            await query.edit_message_text("🔢 Please enter the minimum population:")
        else:
            search_data["min_population"] = CITY_TYPES.get(city_type, 100000)
            await execute_search(update, context, search_data)
        return

async def ask_overwrite_sheet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reply_target = get_reply_target(update)
    keyboard = [
        [
            InlineKeyboardButton("🔁 Overwrite", callback_data="sheet_overwrite:True"),
            InlineKeyboardButton("➕ Append", callback_data="sheet_overwrite:False"),
        ]
    ]
    if reply_target:
        await reply_target.reply_text(
            "Do you want to overwrite the Google Sheet or append to it?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )


async def ask_continue_search(update: Update, context: ContextTypes.DEFAULT_TYPE, results:int = None):
    reply_target = get_reply_target(update)
    keyboard = [
        [
            InlineKeyboardButton("✅ Yes", callback_data="task_continue:True"),
            InlineKeyboardButton("❌ No", callback_data="task_continue:False"),
        ]
    ]
    if reply_target:
        await reply_target.reply_text(
            f"❓ Found {results} cities with given criteria.\nIt would use approximately 120+ API requests per city.\nDo you want to continue with the search?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def handle_sheet_overwrite(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    task_state = data.split(":")[1] == "True"
    params = context.user_data.get("pending_sheet_params", {})
    if not params:
        return
    user_id = params.get("user_id")
    keyword = params.get("keyword")
    state = params.get("state")
    city_type = params.get("city_type")
    city_name = params.get("city_name")
    reply_target = get_reply_target(update)

    with SessionLocal() as db:
            user = db.query(User).filter_by(user_id=user_id).first()
            if not user:
                if reply_target:
                    await reply_target.reply_text("❌ User not found.")
                return
            try:
                sheet_url = create_google_sheet(
                    user.google_sheet_id,
                    task_state,
                    user.email,
                    keyword,
                    state,
                    city_type,
                    city_name
                )
                context.user_data["pending_sheet_params"] = None
                if reply_target:
                    await reply_target.reply_text(f"✅ Your Google Sheet:\n{sheet_url}")
            except Exception as e:
                if reply_target:
                    await reply_target.reply_text(f"❌ Error occurred: {str(e)}")


async def handle_continue_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    reply_target = get_reply_target(update)
    task_continue = data.split(":")[1] == "True"
    if not task_continue:
        reply_target = get_reply_target(update)
        if reply_target:
            await reply_target.reply_text("❌ Search cancelled.")
        return

    search_data = context.user_data.get("search_data", {})
    if not search_data:
        if reply_target:
            await reply_target.reply_text("❌ No search data found. Please start a new search.")
        return
    
    keyword = search_data.get("keyword")
    state = search_data.get("state")
    city_type = search_data.get("city_type")
    city_name = search_data.get("city_name")
    min_population = search_data.get("min_population")
    filename = search_data.get("filename")
    user_id = str(update.effective_user.id)
    message = f"🔁 Started collection for keyword: `{keyword}`"
    if state != "ALL":
        message += f" in `{state}`"
    if city_type and city_type != "all":
        message += f" ({city_type} cities)"
    if city_name:
        message += f", city: {city_name}"

    if reply_target:
        await reply_target.reply_text(message, parse_mode="Markdown")

    try:
        task_id = run_collector_in_thread(keyword, state, city_type, city_name, user_id, filename=filename)
        wait_for_task(task_id)
        context.user_data["pending_sheet_params"] = {
            "user_id": user_id,
            "keyword": keyword,
            "state": state,
            "city_type": city_type,
            "city_name": city_name,
        }            
        await ask_overwrite_sheet(update, context)
    except Exception as e:
        if reply_target:
            await reply_target.reply_text(f"❌ Error occurred: {str(e)}")