import os
import re
import time
import random
import json
import threading
import pandas as pd
import pdfplumber
import requests
import logging
from typing import Optional, Tuple, Dict, List, Any

from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, unquote
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, ElementClickInterceptedException
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from concurrent.futures import ThreadPoolExecutor, as_completed
# 移除了未使用的zipfile和mimetypes

# ========= 日志配置 (Logging Configuration) ==========
# 设置日志级别和格式，同时输出到文件和控制台
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ========= 核心参数配置 (Configuration Class) ==========
class Config:
    """
    集中管理所有配置参数。
    请根据你的项目实际情况修改 PROJECT_DIR 和其他路径。
    """
    # 路径配置
    PROJECT_DIR = Path("/Volumes/ZimingYe/A_project/0928-规则集数据")
    EXCEL_PATH = PROJECT_DIR / "/Volumes/ZimingYe/A_project/第0轮数据采集/规则集数据抓取.xlsx"
    SAVE_DIR = PROJECT_DIR / "0928-规则集数据-output_texts"
    PDF_SAVE_DIR = PROJECT_DIR / "0928-规则集数据-output_pdfs"
    CSV_OUTPUT = PROJECT_DIR / "0928-规则集数据.csv"
    TEMP_DIR = PROJECT_DIR / "temp"
    
    # 支持的输入文件格式
    SUPPORTED_FORMATS = ['.csv', '.xlsx', '.xls']
    
    # 可能的URL列名（按优先级排序），用于智能查找
    URL_COLUMN_CANDIDATES = [
        "Public access URL", "URL", "url", "link", "Link", "网址", "链接",
        "Policy URL", "Document URL", "Source URL"
    ]
    
    # ChromeDriver路径（自动检测，请确保你的路径包含在内或已添加到系统PATH）
    CHROMEDRIVER_PATHS = [
        "/opt/homebrew/bin/chromedriver",  # macOS Homebrew
        "/usr/local/bin/chromedriver",     # 通用路径
        "/usr/bin/chromedriver",           # Linux
        "chromedriver",                    # PATH中
        "./chromedriver"                   # 当前目录
    ]

    # 爬虫行为配置
    MAX_THREADS = 3  # 最大并发处理URL数量，建议保持较低以提高稳定性
    PDF_DOWNLOAD_LIMIT = 10  # 每个URL最多下载的PDF文档数
    PAGE_LOAD_TIMEOUT = 45  # 页面加载超时时间（秒）
    PDF_DOWNLOAD_TIMEOUT = 120  # PDF下载超时时间（秒）
    RANDOM_DELAY_MIN = 2  # 随机延迟最小时间（秒）
    RANDOM_DELAY_MAX = 5  # 随机延迟最大时间（秒）
    MAX_RETRIES = 3  # 网络请求和核心处理的最大重试次数
    
    # 智能导航配置
    ENABLE_SMART_NAVIGATION = True  # 是否启用智能导航功能（使用Selenium递归查找AI子页面）
    MAX_NAVIGATION_DEPTH = 2  # 最大导航深度 (0: 仅当前页; 1: 当前页+一层子页面)
    MAX_AI_LINKS_PER_PAGE = 3  # 每页最多跟踪的AI相关链接数
    MIN_CONTENT_LENGTH = 200  # 页面内容最小长度才保存（防止保存空页或导航页）
    
    # 弹窗处理配置
    POPUP_DETECTION_TIMEOUT = 3  # 弹窗检测超时时间（秒）
    MAX_POPUP_ATTEMPTS = 5  # 最大弹窗处理尝试次数
    
    # 文件大小限制（MB）
    MAX_PDF_SIZE_MB = 50

    # 特定的AI和治理关键词，用于判断相关性
    AI_GOVERNANCE_KEYWORDS = [
        "artificial intelligence", "AI", "machine learning", "neural network",
        "deep learning", "automated decision", "algorithmic system",
        "data-driven", "intelligent system", "automated system",
        "AI governance", "AI policy", "AI strategy", "AI ethics",
        "digital transformation", "algorithmic accountability",
        "AI regulation", "AI guidelines", "responsible AI",
        "digital policy", "technology policy", "innovation policy"
    ]

# 高质量用户代理池 (User Agents)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15"
]

# 线程锁和全局变量
driver_lock = threading.Lock() # 用于保护浏览器驱动初始化过程
session_cookies = {} # 存储会话cookies

# --- 文件格式兼容性处理 (File Handling) ---
def detect_and_read_file(file_path: Path) -> pd.DataFrame:
    """智能检测并读取多种格式的文件（CSV/Excel）"""
    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")
    
    file_ext = file_path.suffix.lower()
    
    try:
        if file_ext == '.csv':
            # 尝试多种编码读取CSV
            encodings = ['utf-8', 'utf-8-sig', 'gbk', 'gb2312', 'iso-8859-1']
            for encoding in encodings:
                try:
                    # 尝试读取，设置sep=None, engine='python' 尝试自动检测分隔符，但通常逗号是标准
                    df = pd.read_csv(file_path, encoding=encoding)
                    logger.info(f"成功读取CSV文件，编码: {encoding}")
                    return df
                except (UnicodeDecodeError, pd.errors.ParserError):
                    continue
            raise Exception("无法识别CSV文件编码或格式")
            
        elif file_ext in ['.xlsx', '.xls']:
            # 读取Excel文件
            engine = 'openpyxl' if file_ext == '.xlsx' else 'xlrd'
            df = pd.read_excel(file_path, engine=engine)
            logger.info(f"成功读取Excel文件: {file_ext}")
            return df
        else:
            raise ValueError(f"不支持的文件格式: {file_ext}")
            
    except Exception as e:
        logger.error(f"❌ 读取文件失败 {file_path}: {e}")
        raise

def find_url_column(df: pd.DataFrame) -> Optional[str]:
    """智能查找包含URL的列名"""
    df.columns = [str(c).strip() for c in df.columns]
    
    # 策略1: 匹配预设的候选列名
    for candidate in Config.URL_COLUMN_CANDIDATES:
        if candidate in df.columns:
            logger.info(f"找到URL列: {candidate}")
            return candidate
    
    # 策略2: 检查列内容，判断是否包含URL格式
    for col in df.columns:
        # 提取前10个非空值进行检查
        sample_values = df[col].dropna().head(10).astype(str)
        # 检查是否有任何值以 http:// 或 https:// 开头
        if not sample_values.empty and sample_values.str.startswith(('http://', 'https://')).any():
            logger.info(f"通过内容推断URL列: {col}")
            return col
    
    return None

# --- 文本清理和处理函数 (Text Processing) ---
def clean_text_for_csv(text: str) -> str:
    """
    增强版文本清理。
    移除特殊字符，合并多余空白，并进行CSV转义，限制长度防止Excel单元格溢出。
    """
    if not text or not isinstance(text, str):
        return ""
    
    # 移除控制字符和特殊字符，替换为单个空格
    text = re.sub(r'[\n\r\f\v\x0b\x0c\t]+', ' ', text)
    text = re.sub(r'[\x00-\x08\x0e-\x1f\x7f-\x9f]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    
    # CSV转义：将双引号替换为两个双引号
    text = text.replace('"', '""')
    
    # 限制长度 (Excel单元格通常限制在32767字符)
    if len(text) > 32000:
        text = text[:32000] + "...[文本被截断]"
    
    return text

def contains_ai_governance_keywords(text: str) -> str:
    """检测AI治理相关关键词并返回匹配信息"""
    if not text or not isinstance(text, str):
        return "无法分析"
    
    text_lower = text.lower()
    # 找到所有匹配的关键词
    matched_keywords = [k for k in Config.AI_GOVERNANCE_KEYWORDS if k.lower() in text_lower]
    
    if len(matched_keywords) >= 3:
        return f"高度相关 (匹配{len(matched_keywords)}个关键词: {', '.join(matched_keywords[:3])}...)"
    elif len(matched_keywords) >= 2:
        return f"相关 (匹配{len(matched_keywords)}个关键词: {', '.join(matched_keywords)})"
    elif len(matched_keywords) == 1:
        return f"可能相关 (匹配关键词: {matched_keywords[0]})"
    else:
        return "不相关"

# --- PDF和文档处理增强 (Document Processing) ---
def is_valid_pdf_url(url: str) -> bool:
    """判断URL是否可能是PDF文档"""
    pdf_indicators = [
        '.pdf', 'filetype=pdf', 'content-type=pdf', '/pdf/', 'document', 'download'
    ]
    url_lower = url.lower()
    return any(indicator in url_lower for indicator in pdf_indicators)

def get_file_info_from_response(response: requests.Response) -> Dict:
    """从HTTP响应中提取文件信息"""
    content_type = response.headers.get('content-type', '').lower()
    content_disposition = response.headers.get('content-disposition', '')
    content_length = response.headers.get('content-length', '0')
    
    # 提取文件名
    filename = None
    if 'filename=' in content_disposition:
        # 使用更稳健的正则匹配文件名
        filename_match = re.search(r'filename\*?=["\']?([^"\';\n]+)', content_disposition)
        if filename_match:
            # unquote 处理URL编码的文件名
            filename = unquote(filename_match.group(1).encode('latin-1').decode('utf-8', 'ignore'))
    
    return {
        'content_type': content_type,
        'filename': filename,
        'size_bytes': int(content_length) if content_length.isdigit() else 0,
        # 检查 Content-Type 或内容前4个字节是否为 %PDF
        'is_pdf': 'pdf' in content_type or response.content[:4] == b'%PDF'
    }

@retry(stop=stop_after_attempt(Config.MAX_RETRIES), wait=wait_exponential(multiplier=1, min=2, max=10), 
       retry_error_callback=lambda retry_state: (None, f"文档下载最终失败: {retry_state.outcome.exception()}", {}))
def download_document_smart(url: str, session: requests.Session, output_dir: Path, 
                          url_index: Any, page_info: Dict = None) -> Tuple[Optional[Path], Optional[str], Dict]:
    """
    智能文档下载，支持多种文档格式，带重试机制。
    url_index 可以是数字或字符串，用于文件名生成。
    """
    if page_info is None:
        page_info = {}
        
    try:
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        # 使用流式GET请求，避免大文件一次性加载到内存
        response = session.get(url, headers=headers, timeout=Config.PDF_DOWNLOAD_TIMEOUT, 
                             stream=True, allow_redirects=True)
        
        if response.status_code != 200:
            # 尝试处理重定向后的URL
            final_url = response.url
            if final_url != url:
                 response = session.get(final_url, headers=headers, timeout=Config.PDF_DOWNLOAD_TIMEOUT, 
                                     stream=True, allow_redirects=True)
                 if response.status_code != 200:
                    return None, f"HTTP状态码: {response.status_code} (最终URL)", {}
            else:
                return None, f"HTTP状态码: {response.status_code}", {}

        file_info = get_file_info_from_response(response)
        
        # 检查文件大小
        if file_info['size_bytes'] > Config.MAX_PDF_SIZE_MB * 1024 * 1024:
            response.close()
            return None, f"文件过大: {file_info['size_bytes']/1024/1024:.1f}MB", file_info
        
        # 生成文件名
        # 优先级：政策标题 > Content-Disposition文件名 > 默认名称
        policy_title = page_info.get('policy_title', '')
        if policy_title:
            base_name = generate_safe_filename(policy_title)[:50]
        elif file_info.get('filename'):
            # 移除文件扩展名，使用 generate_safe_filename
            name_part = Path(file_info['filename']).stem
            base_name = generate_safe_filename(name_part)[:50]
        else:
            base_name = f"document_{url_index}"
        
        # 确定文件扩展名
        if file_info['is_pdf']:
            extension = '.pdf'
        elif 'html' in file_info['content_type']:
            extension = '.html'
        elif 'xml' in file_info['content_type']:
            extension = '.xml'
        else:
            # 尝试从原始文件名推断扩展名
            ext_from_url = Path(urlparse(url).path).suffix.lower()
            extension = ext_from_url if ext_from_url in ['.pdf', '.doc', '.docx', '.txt', '.rtf'] else '.pdf'
            
        # 确保文件名唯一性
        filename = f"{url_index}_{base_name}{extension}"
        file_path = output_dir / filename

        # 检查文件是否已存在（避免重复下载）
        if file_path.exists() and file_path.stat().st_size > 1024:
            logger.info(f"文件已存在，跳过下载: {filename}")
            # 此时需要重新构建file_info，因为是从磁盘读取
            file_info['size_bytes'] = file_path.stat().st_size
            return file_path, None, file_info

        # 下载文件
        total_size = 0
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    total_size += len(chunk)

        response.close() # 必须关闭连接
        
        # 验证下载的文件大小
        if total_size == 0:
            file_path.unlink(missing_ok=True)
            return None, "下载文件内容为空", file_info

        # PDF魔术字节验证
        if extension == '.pdf' and not file_path.read_bytes()[:4] == b'%PDF':
            file_path.unlink(missing_ok=True) # 删除无效文件
            return None, "下载的文件不是有效的PDF (魔术字节检查失败)", file_info

        # 保存元数据（可选，用于调试）
        metadata = {
            'url': url,
            'filename': filename,
            'download_time': time.strftime('%Y-%m-%d %H:%M:%S'),
            'file_info': file_info,
            'page_info': page_info or {},
            'actual_size': file_path.stat().st_size
        }
        metadata_path = file_path.with_suffix('.json')
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

        logger.info(f"成功下载文档: {filename} ({file_info['size_bytes']/1024:.1f}KB)")
        return file_path, None, file_info

    except requests.exceptions.Timeout:
        raise
    except requests.exceptions.RequestException as e:
        raise
    except Exception as e:
        logger.error(f"下载过程中发生未知异常: {str(e)}")
        return None, f"下载失败: {str(e)}", {}

def extract_text_from_document(file_path: Path) -> str:
    """从多种文档格式中提取文本"""
    try:
        if file_path.suffix.lower() == '.pdf':
            return extract_pdf_text_robust(file_path)
        elif file_path.suffix.lower() in ['.html', '.htm']:
            return extract_html_text(file_path)
        elif file_path.suffix.lower() == '.xml':
            return extract_xml_text(file_path)
        else:
            # 尝试作为文本文件读取 (处理.doc/.docx需要额外库，此处仅做简单文本读取)
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()
    except Exception as e:
        return f"[ERROR] 文本提取失败: {str(e)}"

def extract_pdf_text_robust(pdf_path: Path) -> str:
    """增强版PDF文本提取：尝试多种策略"""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            texts = []
            
            for page_num, page in enumerate(pdf.pages):
                page_text = ""
                
                # 策略 1: 标准文本提取 (最常用)
                try:
                    page_text = page.extract_text()
                except Exception:
                    pass
                
                if not page_text or len(page_text.strip()) < 10:
                    # 策略 2: 布局保持提取 (保留更精确的布局)
                    try:
                        page_text = page.extract_text(layout=True, x_tolerance=2, y_tolerance=2)
                    except Exception:
                        pass
                
                if not page_text or len(page_text.strip()) < 10:
                    # 策略 3: 表格提取 (补充表格内容)
                    try:
                        tables = page.extract_tables()
                        if tables:
                            table_texts = []
                            for table in tables:
                                table_text = "\n".join([
                                    " | ".join([str(cell) if cell else "" for cell in row])
                                    for row in table
                                ])
                                table_texts.append(table_text)
                            # 将表格内容追加到当前文本
                            if texts:
                                page_text = texts.pop().split('\n\n')[0] + "\n\n" + "\n\n".join(table_texts) # 尝试与上一页合并
                            else:
                                page_text = "\n\n".join(table_texts)
                    except Exception:
                        pass

                if page_text and len(page_text.strip()) > 5:
                    texts.append(f"[页面 {page_num + 1}]\n{page_text}")
                
            full_text = "\n\n".join(texts)
            
            if not full_text.strip():
                return f"[WARNING] PDF解析成功但未能提取有效文本，可能是扫描版或图像PDF"
            
            return full_text
            
    except Exception as e:
        return f"[ERROR] PDF解析失败: {str(e)}"

def extract_html_text(html_path: Path) -> str:
    """从HTML文件提取文本"""
    try:
        with open(html_path, 'r', encoding='utf-8', errors='ignore') as f:
            soup = BeautifulSoup(f.read(), 'html.parser')
        
        # 移除脚本、样式和导航等非主要内容
        for element in soup(["script", "style", "nav", "header", "footer", "aside", "form", "button"]):
            element.decompose()
        
        return soup.get_text(strip=True, separator=' ')
    except Exception as e:
        return f"[ERROR] HTML文本提取失败: {str(e)}"

def extract_xml_text(xml_path: Path) -> str:
    """从XML文件提取文本"""
    try:
        with open(xml_path, 'r', encoding='utf-8', errors='ignore') as f:
            soup = BeautifulSoup(f.read(), 'xml')
        return soup.get_text(strip=True, separator=' ')
    except Exception as e:
        return f"[ERROR] XML文本提取失败: {str(e)}"

# --- Selenium和浏览器管理 (Selenium and Browser Management) ---
def find_chromedriver_path() -> Optional[str]:
    """自动查找ChromeDriver路径"""
    for path in Config.CHROMEDRIVER_PATHS:
        if Path(path).exists() and os.access(path, os.X_OK):
            logger.info(f"找到ChromeDriver: {path}")
            return path
    
    logger.warning("未找到ChromeDriver，请安装或配置路径")
    return None

def init_chrome_driver_stealth() -> Optional[webdriver.Chrome]:
    """初始化隐身版Chrome Driver，完全无头模式，包含反检测配置"""
    chromedriver_path = find_chromedriver_path()
    if not chromedriver_path:
        return None
    
    logger.info("🚀 初始化隐身Chrome浏览器...")
    
    options = webdriver.ChromeOptions()
    
    # 基础隐身配置
    options.add_argument("--headless=new")  # 使用新的无头模式
    options.add_argument("--no-sandbox") # Linux环境必备
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    # options.add_argument("--disable-images")  # 不加载图片（可选，可以加速但可能影响动态内容）
    # options.add_argument("--disable-javascript")  # 可选：禁用JS会影响动态网站
    
    # 反检测配置
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    
    # 窗口和显示配置
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-notifications")
    
    # 随机用户代理
    options.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")
    
    # 下载配置（防止PDF在浏览器内打开，并设置下载目录）
    download_prefs = {
        "download.default_directory": str(Config.PDF_SAVE_DIR.absolute()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True, # 关键：让PDF直接下载
        "profile.default_content_setting_values.notifications": 2,
        "profile.default_content_settings.popups": 0
    }
    options.add_experimental_option("prefs", download_prefs)
    
    # 日志配置
    options.add_argument("--log-level=3")  # 只显示致命错误
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    try:
        service = Service(executable_path=chromedriver_path)
        driver = webdriver.Chrome(service=service, options=options)
        
        # 设置超时
        driver.set_page_load_timeout(Config.PAGE_LOAD_TIMEOUT)
        driver.implicitly_wait(10) # 隐式等待
        
        # 反检测脚本：移除webdriver标志
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        logger.info("✅ 隐身Chrome浏览器初始化成功")
        return driver
        
    except Exception as e:
        logger.error(f"❌ Chrome浏览器初始化失败: {e}")
        return None

def handle_comprehensive_popups(driver: webdriver.Chrome) -> bool:
    """全面处理各种弹窗：cookies、隐私、订阅、广告等"""
    handled_popup = False
    attempt_count = 0
    
    # 定义各种弹窗处理规则（使用XPATH和CSS Selector）
    popup_handlers = [
        # Cookie 同意/接受 按钮
        {
            'name': 'Cookie同意',
            'selectors': [
                "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept')]",
                "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'agree')]",
                "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'allow')]",
                ".cookie-accept", "#cookie-accept", ".accept-cookies", "#accept-cookies", 
                ".cookie-banner button", ".cookie-consent button", ".gdpr-accept", ".consent-accept"
            ]
        },
        # 关闭弹窗 (X 按钮) 或 “稍后”/“不，谢谢”
        {
            'name': '关闭/跳过',
            'selectors': [
                "//button[contains(@class, 'close')]", "//span[contains(@class, 'close')]",
                "//button[@aria-label='Close']", ".modal-close", ".popup-close", ".dialog-close",
                "[aria-label='Close']", "[data-dismiss='modal']",
                "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'no thanks')]",
                "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'skip')]",
                "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'later')]",
                "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'not now')]",
                ".newsletter-dismiss", ".subscription-close", ".newsletter-close"
            ]
        },
    ]
    
    while attempt_count < Config.MAX_POPUP_ATTEMPTS and not handled_popup:
        attempt_count += 1
        
        for handler in popup_handlers:
            for selector in handler['selectors']:
                try:
                    # 使用较短的超时时间来快速检测弹窗
                    if selector.startswith("//"):
                        # XPATH 查找
                        elements = WebDriverWait(driver, Config.POPUP_DETECTION_TIMEOUT).until(
                            EC.presence_of_all_elements_located((By.XPATH, selector))
                        )
                    else:
                        # CSS Selector 查找
                        elements = WebDriverWait(driver, Config.POPUP_DETECTION_TIMEOUT).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                        )
                    
                    # 尝试点击所有匹配的元素
                    for element in elements:
                        if element.is_displayed() and element.is_enabled():
                            try:
                                # 滚动到元素位置
                                driver.execute_script("arguments[0].scrollIntoView(true);", element)
                                time.sleep(0.5)
                                
                                # 尝试普通点击
                                element.click()
                                logger.info(f"✅ 成功处理{handler['name']}弹窗 (普通点击)")
                                handled_popup = True
                                time.sleep(1)
                                break
                                
                            except ElementClickInterceptedException:
                                # 如果点击被拦截，尝试JS点击
                                try:
                                    driver.execute_script("arguments[0].click();", element)
                                    logger.info(f"✅ 成功处理{handler['name']}弹窗 (JS点击)")
                                    handled_popup = True
                                    time.sleep(1)
                                    break
                                except Exception as js_error:
                                    logger.debug(f"JS点击也失败: {js_error}")
                                    continue
                            except Exception as click_error:
                                logger.debug(f"点击失败: {click_error}")
                                continue
                        
                        if handled_popup:
                            break
                            
                except TimeoutException:
                    continue
                except Exception as e:
                    logger.debug(f"弹窗处理异常: {e}")
                    continue
            
            if handled_popup:
                break
        
        # 这一轮没有处理到弹窗，稍微等待一下再尝试
        if not handled_popup and attempt_count < Config.MAX_POPUP_ATTEMPTS:
            time.sleep(1)
    
    # 额外尝试：按ESC键关闭可能的弹窗
    if not handled_popup and attempt_count == Config.MAX_POPUP_ATTEMPTS:
        try:
            from selenium.webdriver.common.keys import Keys
            driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
            logger.info("🔄 尝试使用ESC键关闭弹窗")
            time.sleep(1)
            handled_popup = True # 假设ESC生效
        except Exception:
            pass
    
    return handled_popup

def find_ai_related_links(soup: BeautifulSoup, base_url: str) -> List[Dict]:
    """智能发现AI相关的子页面链接，用于智能导航"""
    ai_links = []
    
    # 查找所有链接
    for link in soup.find_all('a', href=True):
        href = link.get('href', '')
        text = link.get_text(strip=True).lower()
        title = link.get('title', '').lower()
        
        if not href or href.startswith('#'):
            continue
            
        full_url = urljoin(base_url, href)
        
        # 检查是否为有效的HTTP链接且不在同一页面
        if not full_url.startswith(('http://', 'https://')):
            continue
        
        # 确保是同一个域名下的链接（防止跳出网站）
        if urlparse(full_url).netloc != urlparse(base_url).netloc:
            continue
            
        # 检查链接文本、标题或URL是否包含AI关键词
        relevance_score = 0
        matched_keywords = []
        
        # 使用Config中的AI关键词
        for keyword in Config.AI_GOVERNANCE_KEYWORDS:
            if keyword in text or keyword in title or keyword in href.lower():
                relevance_score += 1
                matched_keywords.append(keyword)
        
        # 只选择相关性较高的链接
        if relevance_score > 0:
            # 排除图片、邮件、电话等非页面链接
            if any(ext in full_url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.mailto', '.tel']):
                 continue
                 
            ai_links.append({
                'url': full_url,
                'text': link.get_text(strip=True)[:100],
                'title': link.get('title', '')[:100],
                'relevance_score': relevance_score,
                'matched_keywords': matched_keywords
            })
    
    # 按相关性排序并去重
    ai_links = sorted(ai_links, key=lambda x: x['relevance_score'], reverse=True)
    seen_urls = set()
    unique_links = []
    
    for link in ai_links:
        # 移除URL末尾的'/'差异
        normalized_url = link['url'].rstrip('/')
        if normalized_url not in seen_urls:
            seen_urls.add(normalized_url)
            unique_links.append(link)
    
    # 限制返回数量
    return unique_links[:Config.MAX_AI_LINKS_PER_PAGE]

def smart_navigate_and_extract(driver: webdriver.Chrome, url: str, max_depth: int) -> Tuple[List[str], List[Dict], List[str]]:
    """智能导航和提取：自动跳转AI相关子页面"""
    extracted_texts = []
    visited_urls = set()
    documents_info = []
    navigation_log = []
    
    def log_and_append(message):
        logger.info(message)
        navigation_log.append(message)

    def extract_from_page(current_url: str, depth: int = 0) -> None:
        """递归提取子函数"""
        # 阻止条件：达到最大深度、已访问过
        if depth > max_depth or current_url.rstrip('/') in visited_urls:
            return
            
        visited_urls.add(current_url.rstrip('/'))
        log_and_append(f"🔍 正在分析页面 (深度 {depth}): {current_url}")
        
        try:
            # 导航到页面
            driver.get(current_url)
            time.sleep(random.uniform(2, 3)) # 导航后等待
            
            # 处理弹窗和动态加载
            handle_page_interactions(driver, current_url)
            
            # 等待页面完全加载
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            
            # 1. 查找文档链接
            doc_links = []
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                link_text = a_tag.get_text(strip=True).lower()
                
                # 检查是否为文档链接
                if any(ext in href.lower() for ext in ['.pdf', '.doc', '.docx', '.txt', '.rtf']) or \
                   any(keyword in href.lower() for keyword in ['download', 'document', 'file', 'attachment']) or \
                   any(keyword in link_text for keyword in ['download', 'pdf', 'document', 'read more']):
                    
                    full_doc_url = urljoin(current_url, href)
                    doc_links.append({
                        'url': full_doc_url,
                        'text': a_tag.get_text(strip=True),
                        'type': 'document'
                    })
            
            documents_info.extend(doc_links)
            
            # 2. 提取当前页面的文本内容
            # 移除不需要的元素
            for element in soup(["script", "style", "nav", "header", "footer", "aside", 
                               "form", "button", "img", ".navigation", ".menu", ".sidebar"]):
                if element:
                    element.decompose()
            
            # 寻找主要内容区域
            main_content = None
            content_selectors = [
                'article', 'main', '.main-content', '.content', '.policy-content',
                '#main-content', '#content', '.document-content', '.text-content',
                '.post-content', '.entry-content', 'body'
            ]
            
            for selector in content_selectors:
                main_content = soup.select_one(selector)
                if main_content and len(main_content.get_text(strip=True)) > Config.MIN_CONTENT_LENGTH:
                    break
            
            page_text = ""
            if main_content:
                page_text = main_content.get_text(strip=True, separator=' ')
            
            # 只保存有足够内容的页面
            if len(page_text) > Config.MIN_CONTENT_LENGTH:
                formatted_text = (
                    f"[页面URL]: {current_url}\n"
                    f"[提取深度]: {depth}\n"
                    f"[提取时间]: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                    f"{page_text}"
                )
                extracted_texts.append(formatted_text)
                log_and_append(f"✅ 从页面提取文本: {len(page_text)} 字符")
            
            # 3. 递归导航到AI相关子链接
            if depth < max_depth:
                ai_links = find_ai_related_links(soup, current_url)
                log_and_append(f"🔗 发现 {len(ai_links)} 个AI相关子链接")
                
                # 访问前N个最相关的子链接
                for ai_link in ai_links:
                    normalized_sub_url = ai_link['url'].rstrip('/')
                    if normalized_sub_url not in visited_urls:
                        log_and_append(f"🎯 跳转到AI相关页面 (得分{ai_link['relevance_score']}): {ai_link['text'][:50]}...")
                        time.sleep(random.uniform(2, 4))  # 随机延迟
                        extract_from_page(ai_link['url'], depth + 1)
                        
        except TimeoutException:
            log_and_append(f"⚠️ 页面加载超时: {current_url}")
        except WebDriverException as e:
            log_and_append(f"⚠️ 浏览器操作失败 {current_url}: {e}")
        except Exception as e:
            log_and_append(f"⚠️ 页面处理失败 {current_url}: {e}")
    
    # 开始递归提取
    extract_from_page(url, 0)
    
    return extracted_texts, documents_info, navigation_log

def handle_page_interactions(driver: webdriver.Chrome, url: str) -> None:
    """处理页面交互：cookies、弹窗、滚动等"""
    try:
        # 1. 使用综合弹窗处理函数
        handle_comprehensive_popups(driver)
        
        # 2. 滚动页面以触发动态加载
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/3);")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight*2/3);")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        
        # 3. 尝试等待动态内容加载（如JavaScript渲染）
        WebDriverWait(driver, 5).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        
    except Exception as e:
        logger.debug(f"页面交互处理异常: {e}")

# --- 核心处理函数 (Main Processing Logic) ---
def process_url_comprehensive(url: str, url_index: int, row_data: Dict = None) -> Tuple[str, int, Dict]:
    """
    综合URL处理函数。
    1. 检查是否为直接PDF。
    2. 启动智能导航（Selenium）递归提取页面内容和文档链接。
    3. 下载并提取发现的文档文本。
    4. 回退到传统网页文本提取（如果前两步失败）。
    """
    logger.info(f"🌐 开始综合处理URL: {url}")
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'DNT': '1',
        'Connection': 'keep-alive'
    })
    
    extracted_text = ""
    pdf_docs_count = 0
    processing_info = {
        'url': url,
        'method': 'unknown',
        'documents_found': 0,
        'pages_visited': 0,
        'ai_links_found': 0,
        'success': False
    }
    
    # 提取页面信息（用于文件名和元数据）
    page_info = {
        'country': row_data.get('Country', 'unknown') if row_data else 'unknown',
        'policy_title': str(row_data.get('Policy initiative ID', f"policy_{url_index}")) if row_data else f"policy_{url_index}",
        'source_url': url
    }
    
    # 尝试 1: 检查是否为直接PDF链接
    if is_valid_pdf_url(url):
        logger.info("🔍 检测到可能的直接PDF链接，尝试直接下载")
        # 直接PDF下载使用重试机制
        try:
            doc_path, error, file_info = download_document_smart(url, session, Config.PDF_SAVE_DIR, url_index, page_info)
            if doc_path:
                text = extract_text_from_document(doc_path)
                if not text.startswith("[ERROR]") and len(text.strip()) > 100:
                    extracted_text = f"=== 文档内容 1 ===\n{text}"
                    pdf_docs_count = 1
                    processing_info.update({
                        'method': 'direct_pdf',
                        'documents_found': 1,
                        'success': True,
                        'file_info': file_info
                    })
                    logger.info("✅ 直接PDF下载和提取成功")
                    return clean_text_for_csv(extracted_text), pdf_docs_count, processing_info
                else:
                    logger.warning(f"⚠️ 直接PDF内容提取问题: {error or '内容过少'}")
            else:
                 logger.warning(f"❌ 直接PDF下载失败: {error}")
        except RetryError as e:
            logger.error(f"❌ 直接PDF下载重试失败: {e.last_attempt.exception()}")
            pass # 继续尝试下一个方法

    # 尝试 2: 使用智能导航处理网页
    driver = None
    try:
        with driver_lock: # 使用锁保护，防止多线程同时初始化浏览器
            driver = init_chrome_driver_stealth()
        
        if not driver:
            raise Exception("无法初始化浏览器驱动")
        
        logger.info("🤖 启动智能导航模式")
        
        page_texts = []
        discovered_docs = []
        navigation_log = []
        
        # 根据配置决定是否使用智能导航
        if Config.ENABLE_SMART_NAVIGATION:
            page_texts, discovered_docs, navigation_log = smart_navigate_and_extract(
                driver, url, max_depth=Config.MAX_NAVIGATION_DEPTH
            )
        else:
            # 传统单页处理
            driver.get(url)
            handle_page_interactions(driver, url)
            
            # 即使禁用智能导航，也尝试提取当前页面的文档链接
            soup = BeautifulSoup(driver.page_source, "html.parser")
            doc_links = []
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if any(ext in href.lower() for ext in ['.pdf', '.doc', '.docx', '.txt', '.rtf']):
                    doc_links.append({'url': urljoin(url, href), 'text': a_tag.get_text(strip=True), 'type': 'document'})
            discovered_docs.extend(doc_links)
        
        # 保存cookies到session，供requests下载文档使用
        cookies = driver.get_cookies()
        for cookie in cookies:
            try:
                session.cookies.set(cookie['name'], cookie['value'], domain=cookie.get('domain'))
            except Exception as e:
                logger.debug(f"设置Cookie失败: {e}")
                
        
        processing_info.update({
            'pages_visited': len(page_texts),
            'ai_links_found': len([d for d in discovered_docs if 'ai' in d.get('text', '').lower()]),
            # 修正：只计算唯一的文档URL
            'documents_found': len(set([d['url'] for d in discovered_docs]))
        })
        
        logger.info(f"📊 智能导航结果: 访问了{len(page_texts)}个页面, 发现{len(discovered_docs)}个文档")
        
        # 下载发现的文档
        successful_texts = []
        if discovered_docs:
            # 去重和过滤无效链接
            unique_docs = {d['url']:d for d in discovered_docs}.values()
            
            logger.info(f"📄 开始下载 {len(unique_docs)} 个发现的文档...")
            
            # 排序：PDF优先，AI关键词多的优先
            sorted_docs = sorted(unique_docs, key=lambda x: (
                'pdf' in x.get('url', '').lower(),
                len([kw for kw in Config.AI_GOVERNANCE_KEYWORDS if kw.lower() in x.get('text', '').lower()])
            ), reverse=True)
            
            # 使用线程池并发下载
            with ThreadPoolExecutor(max_workers=Config.MAX_THREADS) as executor:
                futures = [
                    executor.submit(download_document_smart, doc['url'], session, Config.PDF_SAVE_DIR, 
                                  f"{url_index}_{i}", page_info)
                    for i, doc in enumerate(sorted_docs[:Config.PDF_DOWNLOAD_LIMIT])
                ]
                
                for i, future in enumerate(as_completed(futures)):
                    try:
                        doc_path, error, file_info = future.result()
                        if doc_path:
                            text = extract_text_from_document(doc_path)
                            if not text.startswith("[ERROR]") and len(text.strip()) > 100:
                                successful_texts.append(text)
                                pdf_docs_count += 1
                                logger.info(f"✅ 文档下载并提取成功: {doc_path.name}")
                            else:
                                logger.warning(f"⚠️ 文档内容提取问题: {error or '内容过少'}")
                        else:
                            logger.warning(f"❌ 文档下载失败: {error}")
                    except Exception as e:
                         logger.error(f"❌ 文档下载并发任务失败: {e}")
        
        # 组合所有提取的内容
        all_texts = []
        
        # 添加文档内容（优先级最高）
        if successful_texts:
            all_texts.extend([f"=== 文档内容 {i+1} ===\n{text}" for i, text in enumerate(successful_texts)])
            processing_info['method'] = 'smart_navigation_with_docs'
        
        # 添加页面内容
        if page_texts:
            all_texts.extend([f"=== 页面内容 {i+1} ===\n{text}" for i, text in enumerate(page_texts)])
            if not successful_texts:  # 如果没有文档，则标记为页面内容
                processing_info['method'] = 'smart_navigation_pages'
        
        if all_texts:
            extracted_text = "\n\n--- 内容分隔符 ---\n\n".join(all_texts)
            processing_info['success'] = True
            logger.info(f"✅ 智能导航成功: 提取了{len(all_texts)}个内容块")
        
        # 尝试 3: 如果智能导航没有结果，回退到传统网页文本提取 (仅针对首页)
        if not extracted_text and not Config.ENABLE_SMART_NAVIGATION:
            logger.info("📝 回退到传统网页文本提取...")
            
            # 如果之前没有访问过首页，现在访问
            if not page_texts:
                driver.get(url)
                handle_comprehensive_popups(driver)
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            
            # 移除不需要的元素
            for element in soup(["script", "style", "nav", "header", "footer", "aside", 
                               "form", "button", "img", ".navigation", ".menu", ".sidebar"]):
                if element:
                    element.decompose()
            
            # 寻找主要内容区域
            main_content = None
            content_selectors = [
                'article', 'main', '.main-content', '.content', '.policy-content',
                '#main-content', '#content', '.document-content', '.text-content', 'body'
            ]
            
            for selector in content_selectors:
                main_content = soup.select_one(selector)
                if main_content:
                    break
            
            webpage_text = ""
            if main_content:
                webpage_text = main_content.get_text(strip=True, separator=' ')
            
            # 清理和格式化网页文本
            if len(webpage_text.strip()) > 200: # 只有内容足够多才使用
                extracted_text = (
                    f"[来源URL]: {url}\n"
                    f"[国家]: {page_info['country']}\n"
                    f"[政策标题]: {page_info['policy_title']}\n"
                    f"[提取时间]: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                    f"{webpage_text}"
                )
                processing_info.update({
                    'method': 'fallback_webpage_text',
                    'success': True
                })
                logger.info("✅ 回退网页文本提取成功")
            
    except Exception as e:
        logger.error(f"❌ URL处理失败: {str(e)}", exc_info=True)
        extracted_text = f"[ERROR] URL处理失败: {str(e)}"
        processing_info['error'] = str(e)
        
    finally:
        if driver:
            try:
                driver.quit() # 确保关闭浏览器实例
            except Exception as e:
                logger.warning(f"关闭浏览器失败: {e}")
    
    # 最终检查和处理
    if not extracted_text or extracted_text.startswith("[ERROR]"):
        if not extracted_text:
            extracted_text = f"[ERROR] 无法从URL提取任何有效内容: {url}"
        processing_info['success'] = False
        processing_info['method'] = 'failed'
    elif not extracted_text.startswith("[ERROR]"):
        # 检查内容质量
        if len(extracted_text.strip()) < 100:
            extracted_text = f"[WARNING] 提取内容过少 ({len(extracted_text)} 字符): {extracted_text}"
            processing_info['success'] = False
            processing_info['method'] = 'low_content'
        else:
            processing_info['success'] = True
    
    # 清理文本，避免CSV问题
    final_cleaned_text = clean_text_for_csv(extracted_text)
    
    return final_cleaned_text, pdf_docs_count, processing_info

def generate_safe_filename(text: str, max_length: int = 50) -> str:
    """生成安全的文件名，用于文本和PDF文件"""
    if not text:
        return "unknown"
    
    # 移除或替换不安全的字符
    safe_text = re.sub(r'[\\/:*?"<>|.,;=+[\]\n\r\t]', '_', str(text))
    safe_text = re.sub(r'_+', '_', safe_text)  # 多个下划线合并为一个
    safe_text = safe_text.strip('_')
    
    # 限制长度
    if len(safe_text) > max_length:
        safe_text = safe_text[:max_length].rstrip('_')
    
    return safe_text if safe_text else "unknown"

def save_processing_results(results: List[Dict], output_path: Path) -> None:
    """保存处理结果到CSV文件"""
    try:
        df = pd.DataFrame(results)
        
        # 定义新增和重要列的顺序
        new_columns = [
            "提取文本", "AI治理相关性", "文件名", "处理状态", 
            "PDF文档数", "处理时间(秒)", "文本长度", "处理方法",
            "访问页面数", "发现文档数", "AI链接数"
        ]
        
        # 确保所有新增列都存在
        for col in new_columns:
            if col not in df.columns:
                df[col] = ''
        
        # 将原始列和新增列合并，并保持顺序
        original_columns = [col for col in df.columns if col not in new_columns]
        # 移除重复列并排序
        final_columns = []
        for col in original_columns + new_columns:
            if col not in final_columns:
                final_columns.append(col)
                
        df = df[final_columns]
        
        # 保存到CSV (使用 utf-8-sig 编码以避免Excel打开乱码)
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        logger.info(f"✅ 结果已保存到: {output_path}")
        
    except Exception as e:
        logger.error(f"❌ 保存结果失败: {e}", exc_info=True)
        raise

def print_summary_statistics(results: List[Dict], total_time: float) -> None:
    """打印处理统计信息"""
    total_count = len(results)
    # 成功处理的定义：提取文本不以 [ERROR] 开头
    success_count = sum(1 for r in results if not r.get('提取文本', '').startswith('[ERROR]'))
    pdf_count = sum(r.get('PDF文档数', 0) for r in results)
    ai_relevant_count = sum(1 for r in results if '相关' in r.get('AI治理相关性', ''))
    
    # 计算平均文本长度（只计算成功提取的文本）
    successful_results = [r for r in results if r.get('文本长度', 0) > 0]
    avg_text_length = sum(r.get('文本长度', 0) for r in successful_results) / len(successful_results) if successful_results else 0
    
    print("\n" + "=" * 80)
    print("📊 处理统计报告")
    print("=" * 80)
    print(f"📋 总处理数量: {total_count}")
    print(f"✅ 成功处理: {success_count} ({success_count/total_count*100:.1f}%)")
    print(f"❌ 处理失败: {total_count - success_count}")
    print(f"📄 下载PDF数: {pdf_count}")
    print(f"🤖 AI治理相关: {ai_relevant_count} ({ai_relevant_count/total_count*100:.1f}%)")
    print(f"📏 平均文本长度: {avg_text_length:.0f} 字符")
    print(f"⏱️ 总用时: {total_time/60:.1f} 分钟")
    print(f"⚡ 平均处理速度: {total_time/total_count:.1f} 秒/个")
    print("=" * 80)

# --- 主执行函数 (Main Execution) ---
def main_worker(df: pd.DataFrame, url_column: str, total_urls: int, start_time: float) -> List[Dict]:
    """主逻辑的工作函数，用于线程池"""
    
    # 将处理逻辑封装到一个函数，以便于在单线程（调试）或多线程（生产）中使用
    def process_single_url(idx_original: int, row: pd.Series) -> Optional[Dict]:
        """处理单个URL的封装函数"""
        url = row[url_column]
        row_dict = row.to_dict()
        
        # 使用'编号'作为主要索引，或使用DataFrame的index
        idx = row_dict.get('编号', idx_original)
        
        # 生成文件名基础
        if 'Country' in row_dict and 'Policy initiative ID' in row_dict:
            country = generate_safe_filename(str(row_dict.get('Country', 'unknown')))
            policy_id = generate_safe_filename(str(row_dict.get('Policy initiative ID', 'unknown')))
            filename_base = f"{country}-{policy_id}"
        else:
            filename_base = f"{idx:04d}"
        
        filename_txt = f"{filename_base}.txt"
        
        logger.info(f"\n--- [{idx_original + 1}/{total_urls}] 处理: {filename_base} ---")
        logger.info(f"🔗 URL: {url}")
        
        processing_start = time.time()
        
        # 核心处理
        extracted_text, pdf_docs_count, processing_info = process_url_comprehensive(
            url, idx, row_dict
        )
        
        processing_time = time.time() - processing_start
        
        # 分析结果
        if extracted_text.startswith("[ERROR]"):
            status = "失败"
            ai_relevance = "处理失败"
            display_text = extracted_text
            text_length = 0
            logger.error(f"❌ 处理失败: {extracted_text}")
            
        elif extracted_text.startswith("[WARNING]"):
            status = "警告"
            ai_relevance = "内容过少"
            display_text = extracted_text
            text_length = len(extracted_text)
            logger.warning(f"⚠️ 处理警告: {extracted_text}")

        else:
            status = f"成功-{processing_info.get('method', 'unknown')}"
            if pdf_docs_count > 0:
                status += f"-{pdf_docs_count}文档"
            
            # 保存文本文件
            try:
                text_file_path = Config.SAVE_DIR / filename_txt
                with open(text_file_path, "w", encoding="utf-8") as f:
                    f.write(extracted_text)
                logger.info(f"💾 文本已保存: {filename_txt}")
                
            except Exception as e:
                logger.error(f"❌ 文本保存失败: {e}")
                extracted_text = f"[ERROR] 文本保存失败: {e}"
                status = "失败-保存异常"
            
            # 分析AI相关性
            ai_relevance = contains_ai_governance_keywords(extracted_text)
            text_length = len(extracted_text)
            
            # 决定显示内容
            if text_length < 1000:  # 短文本直接显示
                display_text = extracted_text
            else:  # 长文本只显示文件引用
                display_text = f"文本内容已保存到文件: {filename_txt} (长度: {text_length} 字符)"
            
            logger.info(f"✅ 处理成功 (方法: {processing_info.get('method', 'unknown')})")
            logger.info(f"📄 文档数: {pdf_docs_count}, 📏 长度: {text_length}, 🤖 相关性: {ai_relevance}")

        # 收集结果
        result_record = {
            **row_dict,
            "提取文本": display_text,
            "AI治理相关性": ai_relevance,
            "文件名": filename_txt,
            "处理状态": status,
            "PDF文档数": pdf_docs_count,
            "处理时间(秒)": round(processing_time, 1),
            "文本长度": text_length,
            "处理方法": processing_info.get('method', 'unknown'),
            "访问页面数": processing_info.get('pages_visited', 0),
            "发现文档数": processing_info.get('documents_found', 0),
            "AI链接数": processing_info.get('ai_links_found', 0)
        }
        
        # 随机延迟，避免被反爬
        delay = random.uniform(Config.RANDOM_DELAY_MIN, Config.RANDOM_DELAY_MAX)
        logger.info(f"😴 休息 {delay:.1f} 秒...")
        time.sleep(delay)
        
        return result_record

    all_results: List[Dict] = []
    
    # 使用线程池并发处理URL
    with ThreadPoolExecutor(max_workers=Config.MAX_THREADS) as executor:
        # 提交所有任务
        future_to_url = {
            executor.submit(process_single_url, idx, row): (idx, row[url_column]) 
            for idx, row in df.iterrows()
        }
        
        success_count = 0
        total_pdf_count = 0
        
        # 收集结果并报告进度
        for i, future in enumerate(as_completed(future_to_url)):
            idx, url = future_to_url[future]
            
            try:
                result = future.result()
                if result:
                    all_results.append(result)
                    
                    # 更新进度信息
                    if not result.get('提取文本', '').startswith('[ERROR]'):
                        success_count += 1
                    total_pdf_count += result.get('PDF文档数', 0)
                    
                    # 打印进度报告
                    progress = (i + 1) / total_urls * 100
                    elapsed_time = time.time() - start_time
                    remaining_time = (elapsed_time / (i + 1)) * (total_urls - i - 1) / 60
                    
                    print(f"\n--- 📊 进度报告 ---")
                    print(f"📊 总体进度: {progress:.1f}% ({i+1}/{total_urls}) | 成功: {success_count} | PDF总数: {total_pdf_count}")
                    if remaining_time > 0:
                        print(f"⏱️  预计剩余时间: {remaining_time:.1f} 分钟")
                    logger.info(f"📊 总体进度: {progress:.1f}% | 成功: {success_count} | PDF总数: {total_pdf_count}")
                    
            except Exception as e:
                logger.error(f"❌ URL {url} 的并发任务失败: {e}", exc_info=True)
                # 添加一个失败记录到结果列表
                all_results.append({
                    **df.iloc[idx].to_dict(), 
                    "提取文本": f"[ERROR] 并发任务异常: {e}",
                    "AI治理相关性": "处理失败",
                    "文件名": f"{df.iloc[idx].get('编号', idx):04d}.txt",
                    "处理状态": "失败-任务异常",
                    "PDF文档数": 0,
                    "处理时间(秒)": round(time.time() - start_time, 1),
                    "文本长度": 0
                })
                
    return all_results

# ... (保持所有原始导入和配置不变) ...

def main():
    """主程序入口 (已增强断点续爬功能)"""
    start_time = time.time()
    
    # 打印配置信息
    print("🚀 增强版OECD.ai文档抓取工具 (支持断点续爬)")
    print("=" * 80)
    print("🔧 当前配置:")
    print(f"   📁 输入文件: {Config.EXCEL_PATH}")
    print(f"   🤖 智能导航: {'启用' if Config.ENABLE_SMART_NAVIGATION else '禁用'}")
    print(f"   🔍 导航深度: {Config.MAX_NAVIGATION_DEPTH}")
    print(f"   🔗 每页AI链接数: {Config.MAX_AI_LINKS_PER_PAGE}")
    print(f"   📄 最大PDF下载: {Config.PDF_DOWNLOAD_LIMIT}")
    print(f"   ⚡ 最大线程数: {Config.MAX_THREADS}")
    print("=" * 80)
    
    logger.info("🚀 启动增强版OECD.ai文档抓取工具")
    
    # 创建必要的目录
    for path in [Config.SAVE_DIR, Config.PDF_SAVE_DIR, Config.TEMP_DIR]:
        try:
            path.mkdir(parents=True, exist_ok=True)
            logger.info(f"📁 确保目录存在: {path}")
        except Exception as e:
            logger.error(f"❌ 创建目录失败 {path}: {e}")
            print(f"❌ 错误: 创建目录失败 {path}: {e}")
            return
    
    # 读取输入文件
    try:
        df = detect_and_read_file(Config.EXCEL_PATH)
        logger.info(f"📖 成功读取文件: {Config.EXCEL_PATH}")
        print(f"📊 读取到 {df.shape[0]} 行数据，{df.shape[1]} 列")
        
    except Exception as e:
        logger.error(f"❌ 读取输入文件失败: {e}", exc_info=True)
        print(f"❌ 错误: 读取输入文件失败: {e}")
        return
    
    # 查找URL列
    url_column = find_url_column(df)
    if not url_column:
        logger.error("❌ 未找到URL列，请检查数据格式")
        print(f"❌ 未找到URL列！可用列: {list(df.columns)}")
        return
    
    print(f"🔗 使用URL列: {url_column}")
    
    # --- 断点续爬核心逻辑 ---
    processed_urls = set()
    old_results: List[Dict] = []
    df_processed = pd.DataFrame()
    
    if Config.CSV_OUTPUT.exists():
        try:
            # 1. 尝试读取上次的输出结果
            df_processed = pd.read_csv(Config.CSV_OUTPUT, encoding="utf-8-sig")
            
            # 2. 筛选已处理成功的 URL
            processed_url_col = find_url_column(df_processed) 
            
            if processed_url_col and '处理状态' in df_processed.columns:
                # 认为包含 '成功' 或 '警告' 的状态为已处理
                success_statuses = ['成功', '警告']
                processed_mask = df_processed['处理状态'].astype(str).str.contains('|'.join(success_statuses), na=False)
                processed_urls = set(df_processed[processed_mask][processed_url_col].astype(str).str.strip().tolist())
                
                # 提取已成功处理的结果作为旧结果
                old_results = df_processed[processed_mask].to_dict('records')
                
                logger.info(f"💾 检测到上次运行结果，发现 {len(processed_urls)} 个已处理成功的URL。")
                print(f"💾 检测到上次运行结果，发现 {len(processed_urls)} 个已处理成功的URL。")
            
        except Exception as e:
            logger.warning(f"⚠️ 读取上次输出结果失败，将重新处理所有URL: {e}")
            old_results = []
            processed_urls = set()

    # 3. 数据预处理：从原始数据中过滤掉已处理的 URL
    original_count = len(df)
    df = df.dropna(subset=[url_column])
    df[url_column] = df[url_column].astype(str).str.strip()
    df = df[df[url_column].str.startswith(('http://', 'https://'))]
    
    # 过滤掉已成功处理的 URL
    df_to_process = df[~df[url_column].astype(str).str.strip().isin(processed_urls)].copy()
    
    # 4. 补充处理上次失败的 URL
    if not df_processed.empty:
        # 找出上次失败的记录
        failed_mask = ~df_processed['处理状态'].astype(str).str.contains('成功|警告', na=False)
        df_failed = df_processed[failed_mask]
        
        if not df_failed.empty:
            failed_urls = set(df_failed[url_column].astype(str).str.strip().tolist())
            
            # 找到原始数据中对应的行
            df_to_reprocess = df[df[url_column].astype(str).str.strip().isin(failed_urls)]
            
            # 将未处理的和失败的合并（DataFrame的concat会自动处理索引）
            df_to_process = pd.concat([df_to_process, df_to_reprocess]).drop_duplicates(subset=[url_column], keep='last')
            logger.info(f"🔄 重新加入 {len(failed_urls)} 个上次处理失败的URL进行重试。")
            print(f"🔄 重新加入 {len(failed_urls)} 个上次处理失败的URL进行重试。")
    
    # 添加编号列（如果不存在）
    if '编号' not in df_to_process.columns:
        df_to_process['编号'] = range(1, len(df_to_process) + 1)
    
    total_urls_to_process = len(df_to_process)
    
    if total_urls_to_process == 0:
        logger.info("🎉 所有 URL 似乎都已成功处理，没有新的待处理项。")
        print("🎉 所有 URL 似乎都已成功处理，没有新的待处理项。")
        return
    
    logger.info(f"📈 开始处理 {total_urls_to_process} 个剩余的有效URL...")
    print(f"📈 开始处理 {total_urls_to_process} 个剩余的有效URL...")
    
    # 核心处理流程
    new_results: List[Dict] = []
    try:
        # 将待处理的DataFrame传入 main_worker
        new_results = main_worker(df_to_process, url_column, total_urls_to_process, start_time)
        
    except KeyboardInterrupt:
        print("\n🛑 用户中断程序，正在保存已处理的结果...")
        logger.info("🛑 用户中断程序")
    except Exception as e:
        print(f"\n💥 程序异常: {e}")
        logger.error(f"💥 程序异常: {e}", exc_info=True)
    
    # 5. 合并并保存最终结果
    
    # 最终结果是：旧的成功结果 + 新处理的结果 (包括新的成功和失败)
    results = old_results + new_results
    
    if results:
        try:
            # 转换为 DataFrame 进行去重，确保最新的结果覆盖旧的（包括旧的失败或旧的成功）
            df_final = pd.DataFrame(results)
            # 以 URL 为依据进行去重，保留最新（后处理）的结果
            df_final = df_final.sort_values(by=url_column, key=lambda x: x.astype(str).str.strip()).drop_duplicates(subset=[url_column], keep='last')
            
            # 由于 save_processing_results 期望一个 List[Dict]，我们转回去
            final_results_list = df_final.to_dict('records') 
            
            save_processing_results(final_results_list, Config.CSV_OUTPUT)
            print(f"💾 最终 {len(df_final)} 条结果已合并保存到: {Config.CSV_OUTPUT}")
        except Exception as e:
            logger.error(f"❌ 合并和保存最终结果失败: {e}")
            print(f"❌ 合并和保存最终结果失败: {e}")
            
    # 打印统计报告
    total_time = time.time() - start_time
    print_summary_statistics(results, total_time)
    
    # 输出路径信息
    print(f"\n📁 输出目录信息:")
    print(f"   📝 文本文件: {Config.SAVE_DIR}")
    print(f"   📄 PDF文件: {Config.PDF_SAVE_DIR}")
    print(f"   📊 结果CSV: {Config.CSV_OUTPUT}")
    print(f"   📋 日志文件: scraper.log")
    
    logger.info("🎉 处理完成！")
    print("\n🎉 处理完成！")
    """主程序入口"""
    start_time = time.time()
    
    # 打印配置信息
    print("🚀 增强版文档抓取工具")
    print("=" * 80)
    print("🔧 当前配置:")
    print(f"   📁 输入文件: {Config.EXCEL_PATH}")
    print(f"   🤖 智能导航: {'启用' if Config.ENABLE_SMART_NAVIGATION else '禁用'}")
    print(f"   🔍 导航深度: {Config.MAX_NAVIGATION_DEPTH}")
    print(f"   🔗 每页AI链接数: {Config.MAX_AI_LINKS_PER_PAGE}")
    print(f"   📄 最大PDF下载: {Config.PDF_DOWNLOAD_LIMIT}")
    print(f"   ⚡ 最大线程数: {Config.MAX_THREADS}")
    print("=" * 80)
    
    logger.info("🚀 启动增强版文档抓取工具")
    
    # 创建必要的目录
    for path in [Config.SAVE_DIR, Config.PDF_SAVE_DIR, Config.TEMP_DIR]:
        try:
            path.mkdir(parents=True, exist_ok=True)
            logger.info(f"📁 确保目录存在: {path}")
        except Exception as e:
            logger.error(f"❌ 创建目录失败 {path}: {e}")
            print(f"❌ 错误: 创建目录失败 {path}: {e}")
            return
    
    # 读取输入文件
    try:
        df = detect_and_read_file(Config.EXCEL_PATH)
        logger.info(f"📖 成功读取文件: {Config.EXCEL_PATH}")
        print(f"📊 读取到 {df.shape[0]} 行数据，{df.shape[1]} 列")
        
    except Exception as e:
        logger.error(f"❌ 读取输入文件失败: {e}", exc_info=True)
        print(f"❌ 错误: 读取输入文件失败: {e}")
        return
    
    # 查找URL列
    url_column = find_url_column(df)
    if not url_column:
        logger.error("❌ 未找到URL列，请检查数据格式")
        print(f"❌ 未找到URL列！可用列: {list(df.columns)}")
        return
    
    print(f"🔗 使用URL列: {url_column}")
    
    # 数据预处理
    original_count = len(df)
    df = df.dropna(subset=[url_column])  # 移除空URL行
    df[url_column] = df[url_column].astype(str).str.strip()
    df = df[df[url_column].str.startswith(('http://', 'https://'))]  # 只保留有效URL
    
    # 添加编号列（如果不存在）
    if '编号' not in df.columns:
        df['编号'] = range(1, len(df) + 1)
    
    total_urls = len(df)
    filtered_count = original_count - total_urls
    
    if filtered_count > 0:
        print(f"⚠️  过滤掉 {filtered_count} 行无效数据 (空URL或非HTTP/S开头)")
        
    logger.info(f"📈 开始处理 {total_urls} 个有效URL...")
    print(f"📈 开始处理 {total_urls} 个有效URL...")
    
    if total_urls == 0:
        logger.error("❌ 没有找到有效的URL进行处理")
        print("❌ 没有找到有效的URL进行处理")
        return
    
    # 核心处理流程
    results = []
    try:
        results = main_worker(df, url_column, total_urls, start_time)
        
    except KeyboardInterrupt:
        print("\n🛑 用户中断程序，正在保存已处理的结果...")
        logger.info("🛑 用户中断程序")
    except Exception as e:
        print(f"\n💥 程序异常: {e}")
        logger.error(f"💥 程序异常: {e}", exc_info=True)
    
    # 保存最终结果
    if results:
        try:
            save_processing_results(results, Config.CSV_OUTPUT)
            print(f"💾 结果已保存到: {Config.CSV_OUTPUT}")
        except Exception as e:
            logger.error(f"❌ 保存最终结果失败: {e}")
            print(f"❌ 保存结果失败: {e}")
    
    # 打印统计报告
    total_time = time.time() - start_time
    print_summary_statistics(results, total_time)
    
    # 输出路径信息
    print(f"\n📁 输出目录信息:")
    print(f"   📝 文本文件: {Config.SAVE_DIR}")
    print(f"   📄 PDF文件: {Config.PDF_SAVE_DIR}")
    print(f"   📊 结果CSV: {Config.CSV_OUTPUT}")
    print(f"   📋 日志文件: scraper.log")
    
    logger.info("🎉 处理完成！")
    print("\n🎉 处理完成！")

if __name__ == "__main__":
    try:
        # 确保主程序异常也能被捕获并记录
        main()
    except KeyboardInterrupt:
        print("\n🛑 用户中断程序")
        logger.info("🛑 用户中断程序")
    except Exception as e:
        print(f"\n💥 程序异常退出: {e}")
        logger.error(f"💥 程序异常退出: {e}", exc_info=True)