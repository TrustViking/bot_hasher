# bot_hasher
# Telegram-bot of the international project CREATIVE SOCIETY for comparing video files by hashcode. 
# Human Life is the Highest Value. 
# CREATIVE SOCIETY is an international project that unites people from over 180 countries on a voluntary basis. 
# The goal of the project is to transition, in a legal and peaceful way, within the shortest possible time, to a new creative format of society worldwide, where human life will be the highest value.

# The environment must have the following constant names:

# Y2B_API_KEY - API_KEY to communicate with youtube and get information about the video on this hosting for the purpose of further downloading.

# TELEGRAM_TOKEN - TOKEN of the Telegram bot we get from @BotFather

# TELEGRAM_API_ID - API_ID that the user receives in core.telegram. Read more at: https://core.telegram.org/api/obtaining_api_id

# TELEGRAM_API_HASH - API_HASH that the user receives in core.telegram. Read more at: https://core.telegram.org/api/obtaining_api_id
#
# adding command line arguments
# '-v', '--folder_video', type=str, help='Video folder'
# '-k', '--folder_kframes', type=str, help='Folder for similar frames'
# '-f', '--log_file', type=str, help='Logging log name'
# '-l', '--log_level', type=str, help='Logging Level'
# '-m', '--hash_factor', type=float, help='Threshold multiplier'
# '-t', '--threshold_keyframes', type=float, help='Keyframes threshold'
# '-z', '--logo_size', type=int, help='Side of square to remove logo'
# '--withoutlogo', action='store_true', help='Remove logo'
#
# Example of use:
# example@example:~$ start_hasher.py --hash_factor 0.2 --threshold_keyframes 0.2