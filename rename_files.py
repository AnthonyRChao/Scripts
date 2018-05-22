import os

for filename in os.listdir("."):
    if filename.endswith("q"):
        os.rename(filename, "q")
