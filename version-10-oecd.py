import os
import re
import time
import random
import json
import threading
import pandas as pd
import pdfplumber
import requests

from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from tenacity import retry, stop_after_attempt, wait_exponential
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========= 核心参数配置 ==========
class Config:
    """集中管理所有配置参数"""
    # 路径配置 (已更新为你的新配置)
    PROJECT_DIR = Path("/Volumes/ZimingYe/A_project")
    EXCEL_PATH = PROJECT_DIR / "oecd-ai-all-ai-policies.csv"
    SAVE_DIR = PROJECT_DIR / "0915-oecd-output_texts"
    PDF_SAVE_DIR = PROJECT_DIR / "0915-oecd-output_pdfs"
    CSV_OUTPUT = PROJECT_DIR / "0915-oecd_results.csv"
    URL_COLUMN = "Public access URL"
    
    # 注意：这里的路径需要确保不包含额外的引号
    LOCAL_CHROMEDRIVER_PATH = "/opt/homebrew/bin/chromedriver"

    # 爬虫行为配置
    MAX_THREADS = 5
    PDF_DOWNLOAD_LIMIT = 5
    PAGE_LOAD_TIMEOUT = 60
    PDF_DOWNLOAD_TIMEOUT = 90
    RANDOM_DELAY_MIN = 3
    RANDOM_DELAY_MAX = 8

    # OECD网站特定的AI和治理关键词
    AI_GOVERNANCE_KEYWORDS = [
        "artificial intelligence", "AI", "machine learning", "neural network",
        "deep learning", "automated decision", "algorithmic system",
        "data-driven", "intelligent system", "automated system",
        "AI governance", "AI policy", "AI strategy", "AI ethics",
        "digital transformation", "algorithmic accountability",
        "AI regulation", "AI guidelines", "responsible AI"
    ]

# 用户代理池
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

# 线程锁
driver_lock = threading.Lock()

# --- 文本清理和处理函数 ---
def clean_text_for_csv(text):
    """
    增强版文本清理，特别针对政策文档
    """
    if not text or not isinstance(text, str):
        return ""
    text = re.sub(r'[\n\r\f\v\x0b\x0c\t]+', ' ', text)
    text = re.sub(r'[\x00-\x08\x0e-\x1f\x7f-\x9f]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    text = text.replace('"', '""')
    return text

def contains_ai_governance_keywords(text):
    """检测AI治理相关关键词并返回匹配信息"""
    if not text or not isinstance(text, str):
        return False
    text_lower = text.lower()
    matched_keywords = [k for k in Config.AI_GOVERNANCE_KEYWORDS if k.lower() in text_lower]

    if len(matched_keywords) >= 2:
        return f"高度相关 (匹配{len(matched_keywords)}个关键词: {', '.join(matched_keywords[:3])})"
    elif len(matched_keywords) == 1:
        return f"相关 (匹配关键词: {matched_keywords[0]})"
    else:
        return False

# --- PDF文本提取增强 ---
def extract_pdf_text_robust(pdf_path):
    """
    增强版PDF文本提取，提供多种策略以提高成功率。
    """
    try:
        with pdfplumber.open(pdf_path) as pdf:
            texts = []
            for page in pdf.pages:
                page_text = ""
                # 策略 1: 标准文本提取
                page_text = page.extract_text()
                
                if not page_text or len(page_text.strip()) < 10:
                    # 策略 2: 布局保持提取
                    page_text = page.extract_text(layout=True)
                
                if not page_text or len(page_text.strip()) < 10:
                    # 策略 3: 表格和图形提取（作为补充）
                    tables = page.extract_tables()
                    if tables:
                        for table in tables:
                            table_text = " | ".join([" ".join(str(cell) if cell else "" for cell in row) for row in table])
                            page_text += " " + table_text

                if page_text and len(page_text.strip()) > 10:
                    texts.append(page_text)
            
            full_text = " ".join(texts)
            if not full_text:
                return f"[WARNING] PDF解析成功但未能提取有效文本"
            
            return full_text
            
    except Exception as e:
        return f"[ERROR] PDF解析失败: {str(e)}"

# --- PDF下载优化 ---
def download_pdf_with_metadata(url, session, output_dir, url_index, page_info=None):
    """
    带元数据的PDF下载函数，将元数据保存为单独的JSON文件。
    """
    try:
        headers = {'User-Agent': random.choice(USER_AGENTS)}
        response = session.get(url, headers=headers, timeout=Config.PDF_DOWNLOAD_TIMEOUT, stream=True)
        
        if response.status_code != 200:
            return None, f"HTTP状态码: {response.status_code}"

        # 检查内容类型和文件头
        content_type = response.headers.get('content-type', '').lower()
        is_pdf_content = 'pdf' in content_type or response.content.startswith(b'%PDF')
        if not is_pdf_content:
            return None, f"不是有效的PDF文件，内容类型: {content_type}"

        # 生成文件名
        policy_title = (page_info.get('policy_title', '') or url.split('/')[-1]).replace('/', '_')
        clean_title = re.sub(r'[\\/:*?"<>|.,;=+[\]]', '_', policy_title)[:50]
        filename = f"{url_index:04d}-{clean_title}.pdf"
        pdf_path = output_dir / filename

        with open(pdf_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        # 保存元数据
        metadata = {
            'url': url,
            'filename': filename,
            'download_time': time.strftime('%Y-%m-%d %H:%M:%S'),
            'country': page_info.get('country'),
            'policy_title': page_info.get('policy_title'),
            'file_size': os.path.getsize(pdf_path)
        }
        metadata_path = pdf_path.with_suffix('.json')
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

        return pdf_path, None
    except requests.exceptions.RequestException as e:
        return None, f"网络请求失败: {str(e)}"
    except Exception as e:
        return None, f"下载失败: {str(e)}"

# --- Selenium和核心处理函数 ---
def init_chrome_driver_local():
    """初始化Chrome Driver，添加更多防检测和稳定性配置"""
    print("🚀 初始化Chrome浏览器...")
    if not Path(Config.LOCAL_CHROMEDRIVER_PATH).exists():
        print(f"❌ ChromeDriver文件不存在: {Config.LOCAL_CHROMEDRIVER_PATH}")
        return None
    
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    prefs = {
        "download.default_directory": str(Config.PDF_SAVE_DIR),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "profile.default_content_setting_values.notifications": 2
    }
    options.add_experimental_option("prefs", prefs)
    
    try:
        service = Service(executable_path=Config.LOCAL_CHROMEDRIVER_PATH)
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(Config.PAGE_LOAD_TIMEOUT)
        print("✅ Chrome浏览器初始化成功")
        return driver
    except Exception as e:
        print(f"❌ Chrome浏览器初始化失败: {e}")
        return None

def handle_oecd_dynamic_content(driver):
    """处理动态内容加载和cookies弹窗"""
    try:
        try:
            cookie_accept = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Accept') or contains(text(), 'agree')]"))
            )
            cookie_accept.click()
            time.sleep(2)
        except TimeoutException:
            pass
        
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        
    except Exception as e:
        print(f"动态内容处理失败: {e}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=10))
def process_oecd_url(url, url_index):
    """专门处理OECD.ai网站URL的优化函数"""
    print(f"🌐 正在处理OECD链接: {url}")
    
    session = requests.Session()
    driver = None
    extracted_text = ""
    pdf_docs_count = 0
    
    try:
        with driver_lock:
            driver = init_chrome_driver_local()
        
        if not driver:
            raise Exception("无法初始化Chrome浏览器")
        
        driver.get(url)
        handle_oecd_dynamic_content(driver)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        cookies = driver.get_cookies()
        for cookie in cookies:
            session.cookies.set(cookie['name'], cookie['value'])
            
        page_info = {
            'country': re.search(r'country=(\w+)', url, re.I).group(1) if re.search(r'country=(\w+)', url, re.I) else 'unknown',
            'policy_title': soup.find('h1').get_text(strip=True) if soup.find('h1') else None
        }

        pdf_links = [urljoin(url, a['href']) for a in soup.find_all('a', href=True) if '.pdf' in a['href'].lower()]
        
        if pdf_links:
            print(f"📄 发现 {len(pdf_links)} 个PDF链接，开始下载...")
            
            successful_texts = []
            with ThreadPoolExecutor(max_workers=Config.MAX_THREADS) as executor:
                futures = [
                    executor.submit(download_pdf_with_metadata, pdf_url, session, Config.PDF_SAVE_DIR, url_index, page_info)
                    for pdf_url in pdf_links[:Config.PDF_DOWNLOAD_LIMIT]
                ]
                for future in as_completed(futures):
                    pdf_path, error = future.result()
                    if pdf_path:
                        text = extract_pdf_text_robust(pdf_path)
                        if not text.startswith("[ERROR]") and len(text.strip()) > 100:
                            successful_texts.append(text)
                            pdf_docs_count += 1
                            print(f"✅ PDF下载并提取成功: {os.path.basename(pdf_path)}")
                        else:
                            print(f"⚠️ PDF内容提取问题: {error or '内容过少'}")
                    else:
                        print(f"❌ PDF下载失败: {error}")
            
            if successful_texts:
                extracted_text = "\n\n--- 文档分隔 ---\n\n".join(successful_texts)
            else:
                extracted_text = f"[WARNING] 无法下载或提取任何PDF内容，将尝试提取网页文本。"
        
        if not extracted_text or extracted_text.startswith("[WARNING]"):
            print("📝 提取网页文本作为备选...")
            for element in soup(["script", "style", "nav", "header", "footer", "aside", "form", "button", "img"]):
                element.decompose()
            
            main_content = soup.find('article') or soup.find(id='main-content') or soup.find('.main-content')
            if main_content:
                webpage_text = main_content.get_text(strip=True)
            else:
                webpage_text = soup.get_text(strip=True)
                
            extracted_text = (f"[国家: {page_info['country']}]\n"
                              f"[政策标题: {page_info['policy_title']}]\n\n"
                              f"{webpage_text}")
                              
    except Exception as e:
        extracted_text = f"[ERROR] OECD URL处理失败: {str(e)}"
        
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                print(f"关闭浏览器失败: {e}")
                
    return clean_text_for_csv(extracted_text), pdf_docs_count

# --- 主程序 ---
def main():
    """主执行函数 - 整合所有功能"""
    print("🚀 启动OECD.ai文档抓取工具")
    
    for path in [Config.SAVE_DIR, Config.PDF_SAVE_DIR]:
        path.mkdir(parents=True, exist_ok=True)
    
    if not Config.EXCEL_PATH.exists():
        print(f"❌ 文件不存在: {Config.EXCEL_PATH}")
        return
        
    try:
        # 读取文件，根据文件扩展名选择不同的读取方法
        if Config.EXCEL_PATH.suffix.lower() == '.csv':
            df = pd.read_csv(Config.EXCEL_PATH)
        else:
            df = pd.read_excel(Config.EXCEL_PATH)
            
        df.columns = [c.strip() for c in df.columns]
        
        required_columns = ['编号', Config.URL_COLUMN]
        # 添加 '编号' 列，如果不存在则自动创建
        if '编号' not in df.columns:
            df['编号'] = range(1, len(df) + 1)
        
        if Config.URL_COLUMN not in df.columns:
            print(f"❌ 缺少必要的列: {Config.URL_COLUMN}")
            return
            
    except Exception as e:
        print(f"❌ 读取文件失败: {e}")
        return
    
    results = []
    success_count, total_pdf_count = 0, 0
    total_urls = len(df)
    
    print(f"📈 开始处理 {total_urls} 个URL...")
    
    for idx, row in df.iterrows():
        url = str(row[Config.URL_COLUMN]).strip()
        file_no = str(row['编号']).zfill(4)
        
        if not url.startswith("http"):
            results.append({**row.to_dict(), "提取文本": "[ERROR] 无效URL", "处理状态": "跳过"})
            continue
            
        print(f"\n--- [{idx + 1}/{total_urls}] 编号: {file_no} ---")
        start_time = time.time()
        
        extracted_text, pdf_docs_count = process_oecd_url(url, idx)
        
        status, ai_relevance, display_text, text_length, filename = "", "", "", 0, f"{file_no}.txt"
        
        if extracted_text.startswith("[ERROR]"):
            status = "失败"
            ai_relevance = "处理失败"
            display_text = extracted_text
            text_length = 0
            print(f"❌ 处理失败: {extracted_text}")
        else:
            status = "成功"
            if pdf_docs_count > 0:
                status += f"-{pdf_docs_count}个PDF"
            else:
                status += "-网页文本"
            
            try:
                with open(Config.SAVE_DIR / filename, "w", encoding="utf-8") as f:
                    f.write(extracted_text)
                print(f"💾 文本已保存: {filename}")
            except Exception as e:
                extracted_text = f"[ERROR] 文本保存失败: {e}"
                status = "失败-保存异常"
            
            ai_relevance = contains_ai_governance_keywords(extracted_text) or "不相关"
            text_length = len(extracted_text)
            display_text = f"文本内容已保存到文件 {filename}"
            if text_length < 500: # 长度较短时显示内容
                display_text = extracted_text
            
            success_count += 1
            total_pdf_count += pdf_docs_count
            print(f"✅ 处理成功, 文本长度: {text_length}, AI相关性: {ai_relevance}")

        processing_time = time.time() - start_time
        
        results.append({
            **row.to_dict(),
            "提取文本": display_text,
            "AI治理相关性": ai_relevance,
            "文件名": filename,
            "处理状态": status,
            "PDF文档数": pdf_docs_count,
            "处理时间(秒)": round(processing_time, 1),
            "文本长度": text_length
        })
        
        print(f"📊 进度: {(idx + 1)/total_urls*100:.1f}% | 成功: {success_count} | PDF总数: {total_pdf_count}")
        time.sleep(random.uniform(Config.RANDOM_DELAY_MIN, Config.RANDOM_DELAY_MAX))

    try:
        result_df = pd.DataFrame(results)
        result_df.to_csv(Config.CSV_OUTPUT, index=False, encoding="utf-8-sig")
        print("\n" + "=" * 60)
        print(f"🎉 处理完成！结果已保存至: {Config.CSV_OUTPUT}")
        print(f"📁 文本文件目录: {Config.SAVE_DIR}")
        print(f"📁 PDF文件目录: {Config.PDF_SAVE_DIR}")
    except Exception as e:
        print(f"❌ 保存结果失败: {e}")

if __name__ == "__main__":
    main()