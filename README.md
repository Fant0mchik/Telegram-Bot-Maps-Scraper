# 🇺🇸 Telegram Location Parser Bot 📍🧾

A powerful and user-friendly **Telegram bot** that helps you collect data about places in selected **U.S. cities** using a **keyword** and export the results to a **Google Sheets** file. 🌐🗂️

---

## 📖 Description

This bot allows users to:
- 🔍 Search for businesses or places by keyword (e.g., "moving company", "delivery copmany").
- 🗺️ Select specific U.S. states and filter by city size (large, medium, small, or manual entry).
- 📥 Automatically collect place details (name, address, phone, etc.) using the Google Places API.
- 📤 Export the collected results into a **Google Sheets** file and share it with the user by email.

Before using the search feature, users must first provide a valid email address to receive the exported sheet. ✉️✅

---

## ✨ Features

- 🔐 Email authentication for users.
- 🔎 Interactive step-by-step location and keyword selection via inline keyboard.
- 📌 Supports different city sizes or manual city input.
- 📥 Google Places API integration for detailed place data.
- 📊 Google Sheets API integration to export and share results.
- 💾 SQLite database to store and update place records.
- 💠 Geocoding API integration for better manual cityes searching
- 🔄 Reusable task system with logging.

---

## 📦 Requirements

- Python 3.10+
- Telegram Bot API token
- Google API Key (Places)
- Google Service Account JSON credentials
- `states.json` file with city location metadata
- Required Python packages (see below)

---

## 🛠️ Installation

1. **Clone the repository:**

```bash
git clone https://your-repo-url.git
cd your-repo
```

2. **Set up your .env file with the following variables:**

```bash
BOT_TOKEN=your_telegram_bot_token
GOOGLE_API_KEY=your_google_api_key
GOOGLE_CREDS_FILE=your_service_account_file.json
RADIUS_METERS=50000
REQUEST_DELAY=2.0
```

3. **Place your token.pickle file**


4. **Run docker-compose**
```bash
sudo docker-compose up --build -d
```

---

## ▶️ Usage

1. **Start the bot:**

```bash
python main.py
```

2. **In Telegram:**

- Use `/start` to view your current email status.

- Use `/setemail` to set or update your email.

- Use `/search` to begin a location search:

  - Enter a keyword (e.g., "logistics").

  - Select a U.S. state.

  - Choose a city size or enter a city manually.

  - Wait while the bot collects and processes data.

  - Receive a link to a Google Sheet with the results.