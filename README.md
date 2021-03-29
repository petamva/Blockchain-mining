# Blockchain mining
Developing and mining a blockchain network and caching to a listening mongoDB

<ol>
<li>In a terminal start and establish connection to mongoDB.</li>
<li>Open the project folder in a different terminal and execute: python3 server.py</li>
<li>Run stream_app.py either from an editor of your choice or in a different terminal execute: python3 stream_app.py</li>
<li>Run mongo.py either from an editor of your choice or in a different terminal execute: python3 mongo.py
If this is your first time running the app you may want to wait a little before running mongo.py, for mongoDB 
to be populated and your queries to yield results.</li>
<li>The 1661-0.txt file has to be in the parent folder</li>
</ol>


This blockchain app is intended for running in a UNIX env. It is written in Python
and more specifically in PySpark and PyMongo.

