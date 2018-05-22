import pyautogui
import time

# make sure the 'Next' button is highlighted prior to running the script !!!

# sleep 5 seconds after starting program
time.sleep(5)

# loop through the commands below
for i in range(1, 74):

    pyautogui.keyDown('shift')
    pyautogui.press('tab')
    pyautogui.keyUp('shift')
    pyautogui.press('space')
    pyautogui.press('2')
    pyautogui.press('enter')
    pyautogui.press('tab')
    pyautogui.press('enter')
    time.sleep(0.5)
    pyautogui.press('enter')
