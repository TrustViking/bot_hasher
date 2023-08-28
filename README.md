# bot_compiler
# Telegram-bot for slicing and assembling video fragments of the international project CREATIVE SOCIETY. 
# Human Life is the Highest Value. 
# CREATIVE SOCIETY is an international project that unites people from over 180 countries on a voluntary basis. 
# The goal of the project is to transition, in a legal and peaceful way, within the shortest possible time, to a new creative format of society worldwide, where human life will be the highest value.

# The environment must have the following constant names:

# Y2B_API_KEY - API_KEY to communicate with youtube and get information about the video on this hosting for the purpose of further downloading.

# TELEGRAM_TOKEN - TOKEN of the Telegram bot we get from @BotFather

# TELEGRAM_API_ID - API_ID that the user receives in core.telegram. Read more at: https://core.telegram.org/api/obtaining_api_id

# TELEGRAM_API_HASH - API_HASH that the user receives in core.telegram. Read more at: https://core.telegram.org/api/obtaining_api_id

# At the first run within bot_sender.py (from pyrogram import Client) it is necessary to pass identification by phone number to use API_ID and API_HASH

# TELEGRAM_GROUP - '-100XXX' ID of a new group, where there is only you and a telegram bot, which is assigned to the role of group administrator and necessarily has the rights to delete messages and see messages in the group. This group is necessary for the final video to be sent there, and the bot sends it to the end user who ordered the video fragment. The bot deletes the forwarded message from the group.
# It is necessary to send several third-party messages to this group so that there is at least a little bit of activity there.
#
#

