# 🤖 BitcoinImpactBot – Telegram Add-on for Home Assistant - by matget

A smart Telegram bot that sends daily Bitcoin updates, including:
- Live BTC price
- Weighted market analysis
- Historical impact graph
- Google Sheets integration (read/write)

---

## 🛠 Requirements

- Home Assistant with local add-on support
- Google Sheets document shared with a service account: link & json file (download from google service)
- Telegram Bot Token & Chat ID
  * you will need to define all the details in the addon's configuration

---

## 🧩 Installation

1. **Clone this repo into your Home Assistant add-ons folder:**
   ```bash
   git clone https://github.com/your-username/home-assistant-addons-smart.git
   ```

2. **Get your Google service account credentials:**
   
✅ Step 1: Create a New Project or Select an Existing One
First create simple google sheet.

At the top right, click on the current project name (or the button that says "Select Project").

Click on "New Project".

Give the project a name.

Click Create.

(If you already have an existing project you want to use, you can continue with that.)

✅ Step 2: Enable the Google Sheets API
Go back to the dashboard (as shown in the image you sent me).

Click on "APIs and Services".

In the left-hand menu, click on “+ ENABLE APIS AND SERVICES”.

In the search bar, type: Google Sheets API

Click on it, then click Enable.

🛡️ Step 3: Create a Service Account
On the left sidebar, go back to the Google Cloud Console Home.

Navigate to IAM & Admin → click on Service Accounts.

Click on + CREATE SERVICE ACCOUNT.

Give it a name like: btc-bot-access.

Click Create and Continue.

🧷 Step 4: Set Permissions
On the "Grant this service account access to project" screen:

Select the role:

Basic → Editor
(or at minimum: "Viewer" + "Sheets API Editor" if you want more restricted access)

Click Continue, then Done.

📄 Step 5: Create a JSON Key
Now go back to the list of Service Accounts.

Click the three dots to the right of your btc-bot-access account.

Select "Manage Keys".

Click "Add Key" → "Create new key".

Choose JSON, then click Create.

🔽 The file will be downloaded to your computer. Save it — you'll need to upload it to Home Assistant or your working environment.

🗂️ Step 6: Share the Google Sheet with the Service Account
Open your Google Sheet.

Copy the email address of your Service Account
(it will look like: your-bot@your-project.iam.gserviceaccount.com)

Share the sheet with this email (just like you would share it with a person), and give it Viewer or Editor access, depending on your needs.

3. **Update the Google Sheet Link & the Json content in addon's configuration**

4. Get the Telegram key and Token:
# 🔐 How to Get Your Telegram Bot Token & Chat ID

To use BitcoinImpactBot, you'll need:

✅ A Telegram Bot Token  
✅ Your personal or group Chat ID

Follow the steps below:

---

## 🧑‍💻 1. Create a Telegram Bot

1. Open Telegram and search for [@BotFather](https://t.me/BotFather)
2. Start the chat and type: /newbot
3. Follow the instructions:
- Choose a display name
- Choose a unique username (must end in `bot`, e.g., `mybtc_bot`)
4. 🎉 BotFather will give you your **Bot Token**:

---

## 🧾 2. Find Your Chat ID

### Option A: Personal Chat
1. Open this link (replace YOUR_TOKEN): https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates
2. Send a message to your bot from your personal Telegram account
3. Refresh the link above in your browser
4. Find the number in `"chat":{"id":...}` → **That is your Chat ID**


---

## 🚀 Running the bot

Install the add-on through Home Assistant > Add-on Store > Local add-ons.

Click **Start** to run the bot. 

## 📱 Bot Commands

| Command     | Description                                           |
|-------------|-------------------------------------------------------|
| `/start`    | Start message and menu                                |
| `/btc`      | Get current BTC update with weighted score and graph  |
| `/csv`      | Submit raw category scores (manual or from GPT)       |
| `/update`   | Re-send latest update from Google Sheets              |
| `/history`  | Show last 5 records                                   |
| `/gptnews`  | (Optional) GPT-based crypto news                      |
| `/help`     | Show command list                                     |

---

## 📝 ChangeLog:
[View Changelog](./btc_bot/CHANGELOG.md)

---

## 💬 Contact

For suggestions, improvements or bug reports – open a pull request or contact me directly!

---

