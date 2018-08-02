import requests
import boto3
import uuid
import time
from datetime import datetime, timezone
from feedgen.feed import FeedGenerator
from bs4 import BeautifulSoup


URL_BASE = 'https://aws.amazon.com/api/dirs/blog-posts/items?order_by=SortOrderValue&sort_ascending=false&limit=25&locale='
URL_EN = 'https://aws.amazon.com/blogs/'
URL_CN = URL_BASE + 'zh_CN'
IGNORE_DAYS = 60 * 60 * 24 * 3

# MAKE SURE to replce the following parameters to your own buckets and keys
S3_BUCKET = 'xwj-rss'
S3_KEY_EN = 'aws_blog_en/rss.xml'
S3_KEY_CN = 'aws_blog_cn/rss.xml'


s3 = boto3.client('s3')

# crawl blog homepage, get all blog categories and fetch blogs in each category
def get_blog_en(url, bucket, key):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'}
    html = requests.get(url, headers=headers)
    soup = BeautifulSoup(html.content, 'lxml')
    categories = soup.select('div[data-id="blog-category"] a')

    fg = FeedGenerator()
    fg.title('AWS Blog RSS')
    fg.link(href='https://aws.amazon.com/blogs')
    fg.description('AWS Blog RSS')

    for category in categories:
        blog_category = category.get_text().strip()
        blog_url = 'https://aws.amazon.com' + category.get('href')
        get_blog_by_category(fg, blog_url, blog_category)
        time.sleep(1)

    response = upload_to_s3(fg, bucket, key)
    return response

# fetch blogs in each category
def get_blog_by_category(fg, url, category):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'}
    html = requests.get(url, headers=headers)    
    soup = BeautifulSoup(html.content, 'lxml')
    blogs = soup.select('article[class="blog-post"]')
    now = datetime.now()
    
    for blog in blogs:
        pub_date = blog.select('footer time')[0].get('datetime')
        dt = datetime.strptime(pub_date[::-1].replace(':', '', 1)[::-1], 
                               '%Y-%m-%dT%H:%M:%S%z')
        now = datetime.now(timezone.utc)
        delta = now - dt
        
        # ignore blogs that are (IGNORE_DAYS) days ago
        if delta.total_seconds() > IGNORE_DAYS:
            continue
        
        title = "[{}] ".format(category) + blog.select('h2 span')[0].get_text()
        link = blog.select('h2 a')[0].get('href')
        
        author = blog.select('span[property="author"]')
        if author:
            author = author[0].get_text()
        content = blog.select('section[class="blog-post-excerpt"] p')[0].get_text()
        guid = uuid.uuid5(uuid.NAMESPACE_URL, link).hex

        fe = fg.add_entry()
        fe.link(href=link)
        fe.title(title)
        fe.id(guid)
        fe.description(content)
        # fe.content(content)
        fe.pubDate(pub_date)
        fe.category({'term': category})
        fe.author({'name': author, 'email': 'aws@amazon.com'})

# fetch chinese blogs from rest in json format
def get_blog_cn(url, bucket, key):
    blogs = requests.get(url)
    fg = FeedGenerator()
    fg.title('AWS Blog RSS')
    fg.link(href='https://aws.amazon.com/blogs')
    fg.description('AWS Blog RSS')

    for blog in blogs.json()['items']:
        if blog['additionalFields']['slug'] == 'all': continue
        
        fe = fg.add_entry()
        fe.link(href=blog['additionalFields']['link'])
        fe.title(blog['additionalFields']['title'])
        fe.id(blog['id'])
        fe.description(blog['additionalFields']['slug'])
        fe.pubDate(blog['additionalFields']['modifiedDate'])

    response = upload_to_s3(fg, bucket, key)
        
    return response

# upload feeds to S3
def upload_to_s3(fg, bucket, key):
    rssfeed = fg.rss_str(pretty=True)
    try:
        response = s3.put_object(ACL='public-read', Body=rssfeed, Bucket=bucket, Key=key)
    except Exception as e:
        print(e)

    return response

def lambda_handler(event, context):
    response = get_blog_en(URL_EN, S3_BUCKET, S3_KEY_EN)    
    response = get_blog_cn(URL_CN, S3_BUCKET, S3_KEY_CN)
    return response

if __name__ == '__main__':
    lambda_handler('', '')


