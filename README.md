# Minecraft-username-sniper


this tool checks Minecrafts username availability, specifically focusing on 4-character usernames. It works through a database containing every possible 4-character combination and checks which usernames are currently available.

Because it uses asynchronous requests with aiohttp, it can check large amounts of usernames much faster than a normal sequential checker.

prerequisites  

Python 3
aiohttp

Install aiohttp with:

```pip3 install aiohttp```

How to Use

Move usernames.txt into the same folder as usernamesniper.py, then run:

```python3 usernamesniper.py usernames.txt```

The script will go through the usernames in the file and display any 4-character Minecraft usernames that are available.
