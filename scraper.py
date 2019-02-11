#!/usr/bin/env python3
# encoding=utf8  
import sys  

reload(sys)  
sys.setdefaultencoding('utf8')
"""
SCRAPE URLS FOR CONTENT
"""
import urllib
import datefinder
import MySQLdb

from datetime import datetime
from bs4 import BeautifulSoup as soup  # HTML data structure
from urllib import urlopen as request  # Web client

now = datetime.now()

class Scraper():

    def __init__(self):
        self.url_source_file = 'urls.txt'
        self.outputted_frontend_url = 'frontend_urls.txt'
        self.undo_sql = 'undo.sql'
        self.urls = self.getFileContents(self.url_source_file)
        self.base_url = ''

        self.db = 'yii2advanced_test'
        self.db_port = 3306
        self.db_char = 'utf8'
        self.db_host = '127.0.0.1'
        self.db_user = 'root'
        self.db_pass = ''

        self.connection = MySQLdb.connect(user=self.db_user, passwd=self.db_pass, host=self.db_host, port=self.db_port, db=self.db)
        self.cursor = self.connection.cursor()

        # Counter

        self.counter = 1

        # Undo SQL
        self.undoLastRun()

        # Truncate All Files
        for file in [self.outputted_frontend_url, self.undo_sql]:
            self.writeFile('', file, True)


    def start(self):
        print('STARTING SCRIPT')

        for url in self.urls:      
            print("(" + str(self.counter) + ")FETCHING : " + url)
            
            try:
                client = request(url)

            except urllib.HTTPError as e:
                if e.code == 404 or e.code == 403:
                    self.writeUrlFile({
                        'title': 'Page Does Not Exist',
                        'slug' : url.split('/')[-1]
                    })

                    print('PAGE DOES NOT EXIST!')
                    continue
            else:
                pagecontent = client.read()
                client.close()

                # Parse Data
                article_content = self.parse(pagecontent)

                if article_content['title']:
                    article_content['slug'] = url.split('/')[-1]
                
                    # Save Data to Database
                    article_content = self.saveToDatabase(article_content)

                    # Display Content Summary
                    article_content = self.displayContent(article_content)

                    # Write Data for Frontend Access
                    self.writeUrlFile(article_content)

                    # Write Data for SQL Undo
                    self.writeUndoSQL(article_content)

                else:
                    print('PAGE COULD NOT BE REACHED OR WAS REDIRECTED')

                self.counter += 1

        print('ENDED SCRIPT RUN')

        pass


    def undoLastRun(self):
        undoSQLRows = self.getFileContents(self.undo_sql)

        for row in undoSQLRows:

            try:
                self.cursor.execute(row)
                self.connection.commit()

            except MySQLdb.Error as error :
                self.connection.rollback() #rollback if any exception occured
                print("MYSQL: Failed inserting record {}".format(error))

            finally:
                #closing database connection.
                if(self.connection.ping(True)):
                    self.cursor.close()
                    self.connection.close()
                    print("MySQL connection is closed")
       
        pass


    def getFileContents(self, fileTarget):
        urls = []

        for line in open(fileTarget, 'r'):
            urls.append(line.strip()) 

        return urls


    def createSlug(self, str):
        newStr = str.decode('string_escape').lower()

        for filter in ["'", " ", "_", "\\", "--", ":", ","]:
            newStr = newStr.replace(filter, "-")

        return newStr


    def saveToDatabase(self, _data):
        print('SAVING ARTICLE TO DATABASE')

        # Sanitize data

        data = dict()

        for key, value in _data.items():
            data.update({key : self.sanitizeString(value)})

        try:
            # Save Article
            sql_insert_article_query = """ 
            INSERT INTO 
            `news_article`
            (
                # `id`,
                # `publicId`, 
                `slug`, 
                `title`,
                # `description`,
                `externalUrl`,
                `publicationDateTime`,
                # `thumbnailUrl`,
                `version`,
                `statusId`,
                # `createdAt`,
                # `updatedAt`,
                `createdBy`,
                `updatedBy`,
                `author`
             ) 
             VALUES 
             (
                # `id`,
                # `publicId`, 
                '{slug}', 
                '{title}', # `title`,
                # `description`,
                NULL, # `externalUrl`,
                '{publish_date}', # `publicationDateTime`,
                # `thumbnailUrl`,
                1, # `version`,
                1, # `statusId`,
                # `createdAt`,
                # `updatedAt`,
                1, # `createdBy`,
                1, # `updatedBy`,
                '{author}'  # `author`
            )""".format(**data)

            result  = self.cursor.execute(sql_insert_article_query)
            self.connection.commit()

            if not result:
                print('SQL : There was a problem inserting the data, query:', sql_insert_article_query.replace("\n", ""))

            select_article_query = """
                SELECT 
                    id
                FROM 
                    `news_article` 
                WHERE 
                    title='{title}' 
                AND
                    slug='{slug}'
                AND
                    publicationDateTime='{publish_date}'
                AND
                    author='{author}'
                LIMIT 1
            """.format(**data)

            self.cursor.execute(select_article_query)
            results = self.cursor.fetchall()
            for row in results: 
                data['article_id'] = row[0]

            # data['content'] = self.convert_to_text(data['content'])

            print("MYSQL: Article inserted successfully")

            # Save Article
            sql_insert_cms_content_query = """
            INSERT INTO 
            `cms_content`
            (
                # `id`,
                `version`, 
                `newsArticleId`, 
                `content`,
                `statusId`,
                # `createdAt`,
                # `updatedAt`,
                `createdBy`,
                `updatedBy`

             ) 
             VALUES 
             (
                # `id`,
                1, 
                {article_id}, 
                '{content}',
                1,
                # `createdAt`,
                # `updatedAt`,
                1,
                1
            )""".format(**data)

            result = self.cursor.execute(sql_insert_cms_content_query)
            self.connection.commit()
            if not result:
                print('SQL : There was a problem inserting the data, query: ' + sql_insert_cms_content_query.replace("\n", ""))

            print("MYSQL: CMS Content inserted successfully")

        except MySQLdb.Error as error :
            self.connection.rollback() #rollback if any exception occured
            print("MYSQL: Failed inserting record {}".format(error))

        finally:
            #closing database connection.
            if(self.connection.ping(True)):
                self.cursor.close()
                self.connection.close()
                print("MySQL connection is closed")
       
        return data

    
    def displayContent(self, article_content):
        if article_content['content']:
            content_display = '<Content Found>'

        else:
            content_display= '<Empty>'

        try:
            if article_content['is_iframe']:
                print('IFRAME: iframe url: ' + article_content['iframe_src'] + ' title: ' + article_content['iframe_title'])

            else: 
                print('ARTICLE: content: ' + content_display + ' title: ' + article_content['title'])
                self.writeFile(article_content['content'], 'html/' + str(self.counter) + ".html", True)

        except UnicodeEncodeError as error:
            print('We could not encode some content:' + str(error))
            pass

        return article_content

    
    def parseHTML(self, html, element, target, html_format=False):
        result = html.find(element, target)

        if result and result is not None:
            if html_format:
                return str(self.sanitizeString(result.prettify()))
            else:                
                return str(self.sanitizeString(result.getText()))
        else:
            return ''


    def parse(self, html):
        article_content = {
            'title'         : '',
            'publish_date'  : '',
            'author'        : '', 
            'content'       : '',
            'youtube_link'  : '',
            'is_iframe'     : '',
            'iframe_src'    : '',
            'iframe_title'  : ''
        } 

        html_soup = soup(html, "html.parser")
        iframes =  html_soup.find_all('iframe')

        article_content['title'] = self.parseHTML(html_soup, 'h1', {"class": "p-article__title"})
        article_content['author'] = self.parseHTML(html_soup, 'div', {"class": "p-article__byline__author"})
        article_content['content'] = self.parseHTML(html_soup, 'div', {"class": "article-content"}, True)

        # Date of publishing

        _date = html_soup.find("div", {"class": "p-article__byline__date"})

        parsed_date = datefinder.find_dates(_date.getText()) if (_date and _date is not None) else datefinder.find_dates(now.isoformat())

        for date in parsed_date:
                article_content['publish_date'] = date

        if not iframes:
            print('No iframes found')
            
        else:
            for iframe in iframes:
                # check if iframe exists
                if iframe.attrs['src'].find("talktalk") != -1:
                    article_content['iframe_src'] = iframe.attrs['src']
                    article_content['is_iframe'] = True

                    response = request(article_content['iframe_src'])
                    iframe_soup = soup(response, "html.parser")

                    article_content['iframe_title'] = self.parseHTML(iframe_soup, 'h1', {"class": "page-title"}) if self.parseHTML(iframe_soup, 'h1', {"class": "page-title"}) else article_content['title']
                    
                    # We skip iframed content and just embed the iframe
                    # article_content['content'] = ''

                if iframe.attrs['src'].find("youtube") != -1:
                    article_content['youtube'] = iframe.attrs['src']
       
        return article_content


    def convertToText(self, _string):
        return str(_string.replace('\\xa0', ' ').replace('\n','\n\n')) if _string and _string is not None else ''


    def sanitizeString(self, _text):
        try:
            if _text and type(_text) not in [bool, int, datetime]:
                try:
                    _text = MySQLdb.escape_string(self.encodeString(_text))
                    return _text if (_text) or _text is not None else ''

                except AttributeError:
                    return _text if (_text) or _text is not None else ''
            
            else:
                return _text if (_text) or _text is not None else ''

        except UnicodeEncodeError as error:
            print('We could not encode some content :' + str(error))
            pass


    def encodeString(self, text):
        try:
            text = str(text.encode('utf8', 'ignore')).strip() if text is not None or text != 'None' else ''
        
        except TypeError:
            return text

        return text


    def writeUrlFile(self, data):
        frontend_url = data['title'] + "\n" + self.base_url + data['slug'] + "\n\n"

        self.writeFile(frontend_url, self.outputted_frontend_url)

        pass


    def writeUndoSQL(self, data):
        delete_article_sql = 'DELETE FROM news_article WHERE id = {}'.format(data['article_id']) + ";\n"
        delete_cms_sql = 'DELETE FROM cms_content WHERE newsArticleId = {}'.format(data['article_id']) + ";\n"

        self.writeFile(delete_article_sql, self.undo_sql)
        self.writeFile(delete_cms_sql, self.undo_sql)

        pass


    def writeFile(self, content, filename, truncate = False):
        if truncate:
            fileopen = open(filename, "w")
            fileopen.write('') 

        fileopen = open(filename, "a+")
        fileopen.write(content) 


if __name__ == '__main__':
    Scraper().start()