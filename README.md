# risk_crawler
针对中文维基百科平台，基于Beautifulsoup搭建跨模态风险知识采集框架，获取中文维基百科上涉政风险词条的文本、图片内容。

## 爬虫说明
维基百科爬虫主要基于beautifulsoup库，对于中文维基百科上涉政的相关风险内容进行爬取。其主要包括两个内容：

zhwiki_all.py：根据中文维基百科上的分类页面，对其进行迭代，找到所有的词条页面链接。

zhwiki_leafs.py：对采集到的词条页面进行解析，获取词条内容，包括：词条结构化表格内容、词条正文、词条相关图片等。

## 接口说明
### zhwiki_all.py中的znWiki类
__init__(self,log_file)：
初始相关配置，如设置日志文件、网页请求超时时间等。
log_file：日志文件名称

getResponseContent(self, url)：
返回页面响应内容。
url：请求页面。

getLists(self)：
根据初始的中文维基百科上的分类页面，对其进行迭代，找到所有的词条页面链接。

### zhwiki_leafs.py中的znWiki类
__init__(self,log_file,epoch_file,save_pre_dir,is_download_img,is_check_crawled,leaves_from)：
初始化基本配置。
log_file：日志文件名称。
epoch_file：断点文件名称。用于当爬虫中断后重新从断点开始续爬。
save_pre_dir：数据保存路径
is_download_img：是否下载词条图片
is_check_crawled：是否检查已经爬取。
leaves_from：词条页面列表来源。

get_leaves(self)：
从词条页面文件读取词条链接列表。

self.get_leaves_title()：
从词条标题文件读取词条标题，并构建词条链接。

spider(self, data)：
爬取入口，用于爬去指定词条的内容。
data：列表，第一个元素是待爬取的词条链接，第二个元素是词条标题。

get_table_info_3(self, url, info_table)：
由spider调用，用于解析词条的表格内容。
url：词条URL
info_table：待解析的词条表格内容

pipeline_save(self,item)：
由spider调用，用于保存词条内容
item：解析完成的词条内容

download_imgs(self,item,media_img_dir)：
由pipeline_save调用，用于下载词条相关图片
item：解析完成的词条内容，含有对应的词条相关图片链接
media_img_dir：图片保存地址
