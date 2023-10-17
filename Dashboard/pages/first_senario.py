import streamlit as st
import pandas as pd
import numpy as np
import mysql.connector
import altair as alt
import matplotlib.pyplot as plt
import datetime
from wordcloud import WordCloud
import plotly.express as px
from urllib.parse import quote_plus
from sqlalchemy_utils import create_database, database_exists
from sqlalchemy import create_engine, MetaData
import pymysql
from sqlalchemy.orm import Session
from sqlalchemy import Table, Column, Integer, BIGINT, String, BigInteger, Boolean, Float, Unicode
from sqlalchemy import ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import LONGTEXT
import streamlit as st
import matplotlib.pyplot as plt

st.set_page_config(
    page_title="Ex-stream-ly Cool App",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# connector to database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="3901",
    database="iranketab_new2",
    auth_plugin='mysql_native_password'
)
mycursor = mydb.cursor()


st.write("""as the number of the books published has increased drastically, the market is huge, but allows for experimenting 
         and finding your own voice and brand.""")
# PLOT NO. 3:
with st.container():
    # st.title("نمایش تعداد کتاب‌ها بر حسب سال انتشار.")
    query_3 = ("""SELECT ad_publish_year AS saal, COUNT(url_id) AS count
                FROM book
                WHERE  ad_publish_year > 1500
                GROUP BY ad_publish_year
                ORDER BY count DESC;""")
    df_3 = pd.read_sql(query_3, mydb)
    st.bar_chart(df_3.set_index("saal"))


# PLOT NO. 1
with st.container():
    st.write("""here, you can observe the most written about genres. select the lower bound and the upperbound
             and see how many books can be found in a genre:""")
    # st.write('### Number of Book in each Tag')
    cols = st.columns(2)
    min_num_filter = cols[0].slider('lower bound', 1, 60000, 50)
    max_num_filter = cols[1].slider('upper bound', 1, 60000, 1000)
    df1 = pd.read_sql_query(
        f'select tag.name, count(*) as count from book join tag_book on book.id = tag_book.book_id join tag on tag_book.tag_id = tag.id  group by tag.name having count between {min_num_filter} and {max_num_filter} order by count;', con=mydb)
    cols[0].bar_chart(df1, x='name', y='count')
    cols[1].write(df1)


with st.container():
    st.write("""and here, you can observe for how many pages, are readers more likely to pick up a book:""")
    # PLOT NO. 6
    with st.container():
        st.title("پراکندگی برای نمایش رابطه بین تعداد صفحات و سال انتشار کتاب‌ها.")
        # st.write("  query 6 :")
        query_6 = ("""SELECT title, page_count, solar_publish_year FROM book
        WHERE page_count > 1 AND page_count < 2000 AND solar_publish_year > 500 AND solar_publish_year < 1403
        ORDER BY page_count DESC;""")
        df_6 = pd.read_sql(query_6, mydb)
        scatterchart = px.scatter(df_6, x='page_count', y='solar_publish_year')
        st.plotly_chart(scatterchart)


st.write("pick the min and max page_count and the scatteredness between oages counts and ratings.")
min_page = st.slider(0, 10000, 100 )
max_page = st.slider(0, 10000, 200)
df = pd.read_sql_query(f'select rate, page_count from book where page_count between {min_page} and {max_page};', con=mydb)
plt.plot(df)

with st.container():
    st.write("""due to inflation, the statistics of people reading these days has declined, mostly due to the high price of
             paper. so here's a look towards the scatterness between prices and page counts:""")
    # PLOT personalized no 1
    #st.title("پراکندگی برای نمایش رابطه بین تعداد صفحات و قیمت کتاب‌ها.")
    #st.write("  query 6 :")
    query_11 = ("""SELECT page_count, price FROM book
    WHERE page_count > 1 AND page_count < 200000000 
    ORDER BY page_count DESC;""")
    df_11 = pd.read_sql(query_11 , mydb)
    scatterchart = px.scatter(df_11, x='price', y='page_count')
    st.plotly_chart(scatterchart)

with st.container():
    st.write("""here's the best publishers who has published the most last year (1402) but also with the most mean of ratings.""")
    # PLOT personalized no 2
    #st.title("پراکندگی برای نمایش رابطه بین تعداد صفحات و قیمت کتاب‌ها.")
    #st.write("  query 6 :")
    query_12 = ("""SELECT p.name as publication_name, AVG(b.rate) AS mean_rate, COUNT(*) AS book_count
    FROM publication AS p
    JOIN publication_connector AS pc ON p.id = pc.publication_id
    JOIN book AS b ON pc.book_id = b.id
    WHERE b.solar_publish_year = 1401
    GROUP BY p.id
    ORDER BY book_count DESC, mean_rate DESC
    LIMIT 10;""")
    df_12 = pd.read_sql(query_12 , mydb)
    st.bar_chart(df_12.set_index("publication_name"))


with st.container():
    # st.altair_chart(ghat_chart, theme="streamlit", use_container_width=True)
    ###################################################################################

    st.markdown("""<style>
                div.st-emotion-cache-12w0qpk.e1f1d6gn2 {
                        border: 1px solid #262730;
                        padding: 10px;
                        border-radius: 10px;
                    }
                div.st-emotion-cache-ml2xh6.e1f1d6gn2 {
                        border: 1px solid #262730;
                        padding: 10px;
                        border-radius: 10px;
                    }
                .st-emotion-cache-1qg05tj.e1y5xkzn3 {
                    font-size: 0;
                    min-height: 0;
                }
                .st-emotion-cache-k7vsyb.e1nzilvr2 {
                    margin-bottom: -30px;
                }
                .st-emotion-cache-nbt3vv.ef3psqc12 {
                    width: 100%;
                }
                </style>""", unsafe_allow_html=True)

    st.title(':mag: :blue[Advanced search]')

    col1, col2 = st.columns([1, 3])

    with col1:
        base_query1 = "SELECT MAX(price) AS max_price, MIN(price) AS min_price, MAX(page_count) AS max_quantity, MIN(page_count) AS min_quantity FROM book"
        mycursor_base1 = mydb.cursor()
        mycursor_base1.execute(base_query1)
        max_price, min_price, max_quantity, min_quantity = mycursor_base1.fetchall()[
            0]

        base_query_cover = "SELECT cover FROM book group by cover"
        mycursor_base_cover = mydb.cursor()
        mycursor_base_cover.execute(base_query_cover)
        cover_all = mycursor_base1.fetchall()
        co_type = []  # cover type
        for co in cover_all:
            if co[0] is not None:
                co_type.append(co[0])

        base_query_ghat = "SELECT ghat FROM book group by ghat"
        mycursor_base_ghat = mydb.cursor()
        mycursor_base_ghat.execute(base_query_ghat)
        ghat_all = mycursor_base1.fetchall()
        ghat_type = []  # cover type
        for ghat in cover_all:
            if ghat[0] is not None:
                ghat_type.append(ghat[0])

        st.write('##### Filters')
        _and = st.checkbox('All filters with and')
        st.write('###### Search in')
        col1_srh, col2_srh = st.columns([3, 3])
        book = st.checkbox('Book name')
        author = st.checkbox('Author name')
        publisher = st.checkbox('Publisher name')
        Translator = st.checkbox('translator name')
        srh_txt = st.text_input("", placeholder="key word")

        start_pub = st.selectbox(
            "Start year",
            (range(0, 1403)),
            index=None,
            placeholder="Start",
        )
        end_pub = st.selectbox(
            "End year",
            (range(0, 1403)),
            index=None,
            placeholder="End",
        )

        start_price, end_price = st.slider(
            'Price',
            value=[min_price, max_price])

        start_page_count, end_page_count = st.slider(
            'Number of pages',
            value=[min_quantity, max_quantity])

        start_score, end_score = st.select_slider(
            'score',
            options=['0', '1', '2',
                     '3', '4', '5'],
            value=('0', '5'))

        Cover_type = st.selectbox(
            "Cover type",
            (co_type),
            index=None,
            placeholder="types",
        )

        ghat_type = st.selectbox(
            "ghat type",
            (ghat_type),
            index=None,
            placeholder="types",
        )

        discount = st.checkbox('Discount')
        available = st.checkbox('Available')

        apply_filter = st.button("Apply filter", type="primary")

    with col2:

        query = "SELECT title, en_title, price, ghat, discount, ISBN, cover, page_count, solar_publish_year, ad_publish_year, edition, fastest_delivery, rate, available, pa.name AS author, pt.name AS translator, pub.name AS publishers from book inner join author on book.id = author.book_id inner join person AS pa on author.person_id = pa.id inner join translator on book.id = translator.book_id inner join person AS pt on translator.person_id = pt.id inner join publication_connector on book.id = publication_connector.book_id inner join publication AS pub on publication_connector.publication_id = pub.id"
        flag = True
        _or_and = " OR"
        if apply_filter:
            if _and:
                _or_and = " AND"
            if book:
                if flag:
                    query = query + " WHERE title LIKE '%" + \
                        srh_txt+"%' "+_or_and+" en_title LIKE '%"+srh_txt+"%'"
                    flag = False
                else:
                    query = query + _or_and + " title LIKE '%"+srh_txt + \
                        "%' "+_or_and+" en_title LIKE '%"+srh_txt+"%'"
            elif author:
                if flag:
                    query = query + " WHERE pa.name LIKE '%" + srh_txt+"%'"
                    flag = False
                else:
                    query = query + _or_and + " pa.name LIKE '%"+srh_txt + "%'"
            elif publisher:
                if flag:
                    query = query + " WHERE pub.name LIKE '%" + srh_txt+"%'"
                    flag = False
                else:
                    query = query + _or_and + " pub.name LIKE '%"+srh_txt + "%'"
            elif Translator:
                if flag:
                    query = query + " WHERE pt.name LIKE '%" + srh_txt+"%'"
                    flag = False
                else:
                    query = query + _or_and + " pt.name LIKE '%"+srh_txt + "%'"
            elif start_pub:
                if flag:
                    query = query + " WHERE solar_publish_year >=" + start_pub
                    flag = False
                else:
                    query = query + _or_and + " solar_publish_year >=" + start_pub
            elif end_pub:
                if flag:
                    query = query + " WHERE solar_publish_year <=" + end_pub
                    flag = False
                else:
                    query = query + _or_and + " solar_publish_year <=" + end_pub
            elif end_pub:
                if flag:
                    query = query + " WHERE solar_publish_year <=" + end_pub
                    flag = False
                else:
                    query = query + _or_and + " solar_publish_year <=" + end_pub
            elif start_price:
                if flag:
                    query = query + " WHERE price >=" + start_price
                    flag = False
                else:
                    query = query + _or_and + " price >=" + start_price
            elif end_price:
                if flag:
                    query = query + " WHERE price <=" + end_price
                    flag = False
                else:
                    query = query + _or_and + " price <=" + end_price
            elif start_page_count:
                if flag:
                    query = query + " WHERE page_count >=" + start_page_count
                    flag = False
                else:
                    query = query + _or_and + " page_count >=" + start_page_count
            elif end_page_count:
                if flag:
                    query = query + " WHERE page_count <=" + end_page_count
                    flag = False
                else:
                    query = query + _or_and + " page_count <=" + end_page_count
            elif start_score:
                if flag:
                    query = query + " WHERE rate >=" + start_score
                    flag = False
                else:
                    query = query + _or_and + " rate >=" + start_score
            elif end_score:
                if flag:
                    query = query + " WHERE rate <=" + end_score
                    flag = False
                else:
                    query = query + _or_and + " rate <=" + end_score
            elif Cover_type:
                if flag:
                    query = query + " WHERE cover =" + Cover_type
                    flag = False
                else:
                    query = query + _or_and + " rate cover =" + Cover_type
            elif ghat_type:
                if flag:
                    query = query + " WHERE ghat =" + ghat_type
                    flag = False
                else:
                    query = query + _or_and + " rate ghat =" + ghat_type
            elif discount:
                if flag:
                    query = query + " WHERE discount > 0"
                    flag = False
                else:
                    query = query + _or_and + " rate discount > 0"
            elif available:
                if flag:
                    query = query + " WHERE discount = 1"
                    flag = False
                else:
                    query = query + _or_and + " rate discount = 1"

        mycursor = mydb.cursor()
        mycursor.execute(query)
        myresult = mycursor.fetchall()

        movies = pd.DataFrame(
            myresult,
            columns=['title', 'en_title', 'price', 'ghat',
                     'discount', 'ISBN', 'cover', 'page_count', 'solar_publish_year',
                     'ad_publish_year', 'edition', 'fastest_delivery', 'rate', 'available', 'author', 'translator', 'publishers'])

        st.dataframe(movies, height=1050)
