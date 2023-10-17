import streamlit as st
import pandas as pd
import numpy as np
import mysql.connector
import altair as alt
import matplotlib.pyplot as plt
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

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="3901",
    database="iranketab_new2",
    auth_plugin='mysql_native_password'
)
mycursor = mydb.cursor()


st.title(':blue[Book Readers:]')
st.write('# Surprise')
st.write('#### If you want to choose a book to :red[READ] follow here...')

# PLOT NO. 3
st.write("""as you observe, more and more books have been getting published these past
         few years.""")
with st.container():
    st.title("نمایش تعداد کتاب‌ها بر حسب سال انتشار.")
    query_3 = ("""SELECT ad_publish_year AS saal, COUNT(url_id) AS count
                FROM book
                WHERE  ad_publish_year > 1500
                GROUP BY ad_publish_year
                ORDER BY count DESC;""")
    df_3 = pd.read_sql(query_3, mydb)
    st.bar_chart(df_3.set_index("saal"))

st.write("""we are here to guide you through to pick your next best read.""")


# PLOT NO.2
st.write("""these are the best selling publications yoou might've heard everyday about:""")
df1 = pd.read_sql_query('''select publication.id, name, count(book.id) as count from publication
join publication_connector on publication.id = publication_connector.publication_id
join book on publication_connector.book_id = book.id
group by publication_connector.publication_id
order by count desc limit 10;''')
st.bar_chart(df1, x='name', y='count')

# PLOT NO. 4
with st.container():
    st.title("نمایش تعداد کتاب‌ها بر حسب 10 نویسنده برتر از نظر تعداد:")
    query_5 = ("""SELECT p.name AS author_name, COUNT( b.url_id) AS url_count, AVG(b.rate) AS mean_rate, COUNT( b.url_id) * AVG(b.rate) AS author_rate
    FROM author t
    JOIN person p ON t.person_id = p.id
    JOIN book b ON t.book_id = b.url_id
    JOIN tag_book ON b.id = tag_book.book_id
    JOIN tag ON tag_book.tag_id = tag.id
    WHERE p.name IS NOT NULL
    GROUP BY t.person_id, p.name
    ORDER BY author_rate DESC
    LIMIT 10;""")
    df_5 = pd.read_sql(query_5, mydb)
    st.write(df_5)
    df_5.drop(['mean_rate'], axis=1, inplace=True)
    df_5.drop(['author_rate'], axis=1, inplace=True)
    st.bar_chart(df_5.set_index("author_name"))


# PLOT NO. 5
with st.container():
    st.title("نمایش تعداد کتاب‌ها بر حسب 10 مترجم برتر از نظر تعداد:")
    query_5 = ("""SELECT p.name AS translator_name, COUNT(DISTINCT b.url_id) AS url_count, AVG(b.rate) AS mean_rate, COUNT(DISTINCT b.url_id) * AVG(b.rate) AS translator_rate
    FROM translator t
    JOIN person p ON t.person_id = p.id
    JOIN book b ON t.book_id = b.id
    WHERE p.name IS NOT NULL
    GROUP BY t.person_id, p.name
    ORDER BY translator_rate DESC
    LIMIT 10;""")
    df_5 = pd.read_sql(query_5, mydb)
    st.write(df_5)
    df_5.drop(['mean_rate'], axis=1, inplace=True)
    df_5.drop(['translator_rate'], axis=1, inplace=True)
    st.bar_chart(df_5.set_index("translator_name"))

# PLOT NO. 6
with st.container():
    st.title("پراکندگی برای نمایش رابطه بین تعداد صفحات و سال انتشار کتاب‌ها:")
    # st.write("  query 6 :")
    query_6 = ("""SELECT title, page_count, solar_publish_year FROM book
    WHERE page_count > 1 AND page_count < 2000 AND solar_publish_year > 500 AND solar_publish_year < 1403
    ORDER BY page_count DESC;""")
    df_6 = pd.read_sql(query_6, mydb)
    scatterchart = px.scatter(df_6, x='page_count', y='solar_publish_year')
    st.plotly_chart(scatterchart)


# PLOT NO. 7
# Ali
with st.container():
    meta = MetaData()
    # Database connection details
    user = 'root'
    password = '3901'  # your password here
    host = 'localhost'
    port = '3306'
    db_name = 'iranketab_new2'

    # Create the database if it doesn't exist
    url = f'mysql://{user}:{password}@{host}:{port}/{db_name}?charset=utf8mb4'
    if not database_exists(url):
        create_database(url)

    # Connect to the database
    engine = create_engine(url, pool_pre_ping=True)
    conn = engine.connect()

    # Load tables into dataframes
    df_book = pd.read_sql("SELECT * FROM book", engine)
    df_publication_connector = pd.read_sql(
        "SELECT * FROM publication_connector", engine)
    df_author = pd.read_sql("SELECT * FROM author", engine)
    df_translator = pd.read_sql("SELECT * FROM translator", engine)
    df_tag_book = pd.read_sql("SELECT * FROM tag_book", engine)

    # Step 1: First Merge
    df_joined = df_book.merge(df_publication_connector, how='left',
                              left_on='id', right_on='book_id', suffixes=('', '_pub'))

    # Step 2: Second Merge
    df_joined = df_joined.merge(
        df_author, how='left', left_on='id', right_on='book_id', suffixes=('', '_auth'))

    # Step 3: Third Merge
    # Updating the column name based on what's actually in df_joined after the previous merges
    df_joined = df_joined.merge(
        df_translator, how='left', left_on='id', right_on='book_id', suffixes=('', '_trans'))

    # Step 4: Fourth Merge
    # Updating the column name based on what's actually in df_joined after the previous merges
    df_joined = df_joined.merge(
        df_tag_book, how='left', left_on='id', right_on='book_id', suffixes=('', '_tag'))

    # Step 5: Display to check
    df_joined.head()
    df_joined_cleaned = df_joined.dropna(subset=['person_id'])

    st.title("Book Analytics Dashboard")

    # Chart 7: Relationship between the price without discount and the year of publication
    st.subheader("Chart 7: Price vs Year of Publication (Focused)")
    fig3, ax3 = plt.subplots()

    # Filter data for years between 1400 and 1600
    df_filtered = df_joined_cleaned[(df_joined_cleaned['solar_publish_year'] >= 1360) & (
        df_joined_cleaned['solar_publish_year'] <= 1405)]

    # Scatter plot
    ax3.scatter(df_filtered['solar_publish_year'], df_filtered['price'])

    # Labels and Titles
    ax3.set_xlabel('Year of Publication (Shamsi)')
    ax3.set_ylabel('Price')
    ax3.set_title('Price vs Year of Publication')
    ax3.grid(True)

    # Log transformation for y-axis
    ax3.set_yscale('log')

    # Show the plot
    st.pyplot(fig3)
    # Further Analysis 1: Yearly Price Trends
    df_filtered_grouped = df_filtered.groupby('solar_publish_year')[
        'price'].agg(['mean', 'median', 'std'])
    fig, ax = plt.subplots()
    df_filtered_grouped.plot(kind='line', ax=ax)
    ax.set_title('Yearly Price Trends')
    ax.set_xlabel('Year of Publication')
    ax.set_ylabel('Price')
    st.pyplot(fig)

    # Further Analysis 2: High-Price and Low-Price Markers
    highest_price_year = df_filtered[df_filtered['price'] ==
                                     df_filtered['price'].max()]['solar_publish_year'].iloc[0]
    lowest_price_year = df_filtered[df_filtered['price'] ==
                                    df_filtered['price'].min()]['solar_publish_year'].iloc[0]
    st.write(
        f"The highest priced book was published in the year {highest_price_year}.")
    st.write(
        f"The lowest priced book was published in the year {lowest_price_year}.")

    # Further Analysis 3: Correlation Analysis
    correlation = df_filtered['solar_publish_year'].corr(df_filtered['price'])
    st.write(
        f"The correlation between the year of publication and the price is {correlation}.")

    # Chart 8: Price dispersion chart based on book score (assuming a 'rate' column for book score)
    st.subheader("Chart 8: Price vs Score")
    fig4, ax4 = plt.subplots()
    ax4.scatter(df_joined_cleaned['rate'],
                df_joined_cleaned['price'], alpha=0.5)
    ax4.set_xlabel('Book Score')
    ax4.set_ylabel('Price')
    ax4.set_yscale('log')  # log scale for y-axis
    ax4.grid(True)  # adding grid lines
    ax4.legend(['Price vs Score'])
    ax4.set_title('Price vs Book Score')
    st.pyplot(fig4)


# Step 9: According to the type of ghat
st.write("# Chart 9: Accordingto the type of ghat")
st.write("###### Display the number of books according to their ghat type")

query_1_9 = "SELECT count(*), ghat FROM book group by ghat"
mycursor_query_1_9 = mydb.cursor()
mycursor_query_1_9.execute(query_1_9)
ghat = mycursor_query_1_9.fetchall()
ghat_count = []
ghat_title = []
for gh in ghat:
    ghat_count.append(gh[0])
    if gh[1] is not None:
        ghat_title.append(gh[1])
    else:
        ghat_title.append("")

df_ghat = pd.DataFrame({
    'title': ghat_title,
    'count': ghat_count
})

ghat_chart = alt.Chart(df_ghat).mark_bar().encode(
    x='title',
    y='count'
)

st.altair_chart(ghat_chart, theme="streamlit", use_container_width=True)
