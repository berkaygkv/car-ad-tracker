from selenium import webdriver



def init_driver(user_agent, user_profile, debug_port=False, load_image=0, headless=True):
    options = webdriver.ChromeOptions()
    options.add_experimental_option('useAutomationExtension', False)
    options.add_experimental_option("excludeSwitches", [
                                    "enable-automation", "ignore-certificate-errors", "enable-logging"])
    options.add_argument("--user-data-dir={}".format(user_profile))
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1280,1080")
    options.add_argument(f"--user-agent={user_agent}")
    options.add_argument("--disable-extensions")
    options.add_argument("--dns-prefetch-disable")
    options.add_argument("--disable-gpu")
    prefs = {"profile.managed_default_content_settings.images": load_image}
    options.add_experimental_option("prefs", prefs)

    if headless:
        options.headless = True

    if debug_port:
        options.add_argument(f"--remote-debugging-port={debug_port}")

    bot = webdriver.Chrome(options=options)
    bot.maximize_window()
    bot.set_page_load_timeout(65)
    
    if debug_port:
        input('Did you access local host? ')

    return bot
