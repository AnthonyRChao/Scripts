"""
I created this script to help my girlfriend screenshot thousands of pages of e-textbooks 
that she did not want to pay for. Incredibly satisfying.
"""
import pyautogui
import time

# make sure the 'Next' button is highlighted prior to running this script!!!

# pause 5 seconds to jump to the correct screen
time.sleep(5)

# loop over the commands below to take screenshots
for i in range(1, 999):

    pyautogui.screenshot('screenshot_%dq.png' % i)
    pyautogui.keyDown('shift')
    pyautogui.press('tab')
    pyautogui.keyUp('shift')
    pyautogui.press('space')
    pyautogui.press('2')
    pyautogui.press('enter')
    pyautogui.press('tab')
    pyautogui.press('enter')
    time.sleep(1.25)
    pyautogui.screenshot('screenshot_%da.png' % i)
    pyautogui.press('enter')
    time.sleep(1.25)
