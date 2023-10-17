import streamlit as st
import pandas as pd
import numpy as np
import mysql.connector
import altair as alt
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from PIL import Image
from scipy.stats import ttest_rel
from scipy.stats import shapiro, levene, ttest_ind
import seaborn as sns
import matplotlib.pyplot as plt

from sqlalchemy import create_engine
import pandas as pd
from urllib.parse import quote_plus
from sqlalchemy_utils import create_database, database_exists
from sqlalchemy import create_engine
from sqlalchemy import MetaData
import pymysql
from sqlalchemy.orm import Session
from sqlalchemy import Table, Column, Integer, BIGINT, String, BigInteger, Boolean, Float, Unicode
from sqlalchemy import ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy import create_engine

import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy.orm import Session
from sqlalchemy import Table, Column, Integer, BIGINT, String, BigInteger, Boolean, Float, Unicode
from sqlalchemy import ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import LONGTEXT
meta = MetaData()
# Database connection details
user = 'root'
password = '3901'  # your password here
host = 'localhost'
port = '3306'
db_name = 'iranketab_new2'

st.set_page_config(
    page_title="Ex-stream-ly Cool App",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
)

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
df_tag = pd.read_sql("SELECT * FROM tag", engine)

# Step 1: First Merge
df_joined = df_book.merge(df_publication_connector, how='left',
                          left_on='id', right_on='book_id', suffixes=('', '_pub'))

# Step 2: Second Merge
df_joined = df_joined.merge(
    df_author, how='left', left_on='id', right_on='book_id', suffixes=('', '_auth'))

# Step 3: Third Merge
df_joined = df_joined.merge(df_translator, how='left',
                            left_on='id', right_on='book_id', suffixes=('', '_trans'))

# Step 4: Fourth Merge (tag_book to get tag_id)
df_joined = df_joined.merge(
    df_tag_book, how='left', left_on='id', right_on='book_id', suffixes=('', '_tag'))

# Step 5: Fifth Merge (tag to get tag name)
df_joined = df_joined.merge(
    df_tag, how='left', left_on='tag_id', right_on='id', suffixes=('', '_tag_name'))


df_joined_filtered = df_joined[df_joined['name'].str.contains(
    'تاریخ', case=False, na=False)]

# Step 7: Group by publication and calculate metrics
df_grouped = df_joined_filtered.groupby('publication_id').agg({
    'id': 'count',  # Number of Books
    'rate': 'mean'  # Average Rating
}).reset_index()

df_grouped.rename(columns={'id': 'num_of_books',
                  'rate': 'avg_rating'}, inplace=True)

# Step 8: Sort by the number of books and average rating
df_grouped = df_grouped.sort_values(
    by=['num_of_books', 'avg_rating'], ascending=[False, False]).head(5)
# Chart: Top 5 Publications for Historical Books Based on Number of Books Published
st.subheader(
    "Top 5 Publications for Historical Books Based on Number of Books Published")
fig1, ax1 = plt.subplots()
sns.barplot(data=df_grouped, x='publication_id', y='num_of_books', ax=ax1)
ax1.set_title(
    "Top 5 Publications Based on Number of Historical Books Published")
ax1.set_xlabel("Publication ID")
ax1.set_ylabel("Number of Books Published")
st.pyplot(fig1)

# Chart: Top 5 Publications for Historical Books Based on Average Rating
st.subheader("Top 5 Publications for Historical Books Based on Average Rating")
fig2, ax2 = plt.subplots()
sns.barplot(data=df_grouped, x='publication_id', y='avg_rating', ax=ax2)
ax2.set_title("Top 5 Publications Based on Average Rating of Historical Books")
ax2.set_xlabel("Publication ID")
ax2.set_ylabel("Average Rating")
st.pyplot(fig2)


# connector to database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="3901",
    database="iranketab_new2",
    auth_plugin='mysql_native_password'
)
mycursor = mydb.cursor()


st.title(':blue[Employers Request:]')
st.write('# Hello')
# image = Image.open('Data_analysis_hero.jpg')
# st.image(image)

st.write('##### In this web page you can get some information about books and their statistical analyze.......')


st.write('### :green[Buyer 1 request:]')
st.write('#### A person who is very interested in romance books asked you to introduce 5 of the best authors of this genre.')

query_d1_1 = "SELECT title, en_title, price, ghat, discount, ISBN, cover, page_count, solar_publish_year, ad_publish_year, edition, fastest_delivery, rate, available FROM book WHERE book.page_count != 0 AND book.rate > 0 AND solar_publish_year != 0 ORDER BY book.rate DESC LIMIT 100;"
mycursor_query_d1_1 = mydb.cursor()
mycursor_query_d1_1.execute(query_d1_1)
best_auther_1000 = mycursor_query_d1_1.fetchall()
df_best_auther_1000 = pd.DataFrame(
    best_auther_1000,
    columns=['title', 'en_title', 'price', 'ghat',
             'discount', 'ISBN', 'cover', 'page_count', 'solar_publish_year',
             'ad_publish_year', 'edition', 'fastest_delivery', 'rate', 'available'])
max_count_page_1000 = df_best_auther_1000['page_count'].max()
min_count_page_1000 = df_best_auther_1000['page_count'].min()
min_rate_1000 = df_best_auther_1000['rate'].min()
max_price_1000 = df_best_auther_1000['price'].max()
max_year_1000 = df_best_auther_1000['solar_publish_year'].max()
min_year_1000 = df_best_auther_1000['solar_publish_year'].min()

query_d1_2 = "SELECT person.name AS auther, tag.name AS tag_name, title, en_title, price, ghat, discount, ISBN, cover, page_count, solar_publish_year, ad_publish_year, edition, fastest_delivery, rate, available FROM book INNER JOIN author ON author.book_id = book.url_id INNER JOIN person ON author.person_id = person.id INNER JOIN tag_book ON book.id = tag_book.book_id INNER JOIN tag ON tag_book.tag_id = tag.id WHERE tag.name = 'عاشقانه' OR tag.name = 'داستان عاشقانه'"

mycursor_query_d1_2 = mydb.cursor()
mycursor_query_d1_2.execute(query_d1_2)
best_auther_5 = mycursor_query_d1_2.fetchall()
df_best_auther_5 = pd.DataFrame(
    best_auther_5,
    columns=['auther', 'tag_name', 'title_book', 'en_title', 'price', 'ghat',
             'discount', 'ISBN', 'cover', 'page_count', 'solar_publish_year',
             'ad_publish_year', 'edition', 'fastest_delivery', 'rate', 'available'])
filtered_df_best_auther_5 = df_best_auther_5[df_best_auther_5['rate'] > int(
    min_rate_1000)]
filtered_df_best_auther_5 = filtered_df_best_auther_5[
    filtered_df_best_auther_5['page_count'] <= max_count_page_1000]
filtered_df_best_auther_5 = filtered_df_best_auther_5[
    filtered_df_best_auther_5['page_count'] >= min_count_page_1000]
filtered_df_best_auther_5 = filtered_df_best_auther_5[
    filtered_df_best_auther_5['price'] < max_price_1000]
filtered_df_best_auther_5 = filtered_df_best_auther_5[
    filtered_df_best_auther_5['solar_publish_year'] >= min_year_1000]
filtered_df_best_auther_5 = filtered_df_best_auther_5[
    filtered_df_best_auther_5['solar_publish_year'] <= max_year_1000]
filtered_df_best_auther_5.sort_values(
    by='rate', ascending=False, inplace=True)
filtered_df_best_auther_5_6 = filtered_df_best_auther_5.head(7)
filtered_df_best_auther_5 = filtered_df_best_auther_5.head(5)

st.write('###### In this question, the characteristics of the top 100 books were discussed first, and then, based on the extracted characteristics, the top 5 authors whose books have such characteristics were examined.')
st.write('#### Features of the top 100 books')
st.write('###### max count page=> ' + str(max_count_page_1000))
st.write('###### min count page=> ' + str(min_count_page_1000))
st.write('###### min rate=> ' + str(min_rate_1000))
st.write('###### max price=> ' + str(max_price_1000))
st.write('###### max year=> ' + str(max_year_1000))
st.write('###### min year=> ' + str(min_year_1000))


st.write(filtered_df_best_auther_5)

scatter_plot = alt.Chart(filtered_df_best_auther_5_6).mark_circle().encode(
    x='auther',
    y='page_count',
    color='rate',
    size='price',
    tooltip=['auther', 'rate',
             'page_count', 'solar_publish_year', 'price']
).interactive()

st.altair_chart(scatter_plot, theme="streamlit", use_container_width=True)


st.write('### :green[Buyer 2 request:]')
st.write('#### Suppose a person who is a bookworm and wants to buy a lot of books but does not have much money for this, so he wants you to introduce him books that are in the first quarter of the best in terms of quality and in the 20% cheapest in terms of price.')

query_q2 = "select title, en_title, price, ghat, discount, ISBN, cover, page_count, solar_publish_year, ad_publish_year, edition, fastest_delivery, rate, available from book"
mycursor_query_q2 = mydb.cursor()
mycursor_query_q2.execute(query_q2)
q2_all = mycursor_query_q2.fetchall()
df_q2_all = pd.DataFrame(
    q2_all,
    columns=['title', 'en_title', 'price', 'ghat',
             'discount', 'ISBN', 'cover', 'page_count', 'solar_publish_year',
             'ad_publish_year', 'edition', 'fastest_delivery', 'rate', 'available'])

# filtering by quality
top_quality_books = df_q2_all[df_q2_all['rate']
                              >= df_q2_all['rate'].quantile(0.75)]

# filtering by price
new_df_q2 = top_quality_books[top_quality_books['price']
                              <= top_quality_books['price'].quantile(0.2)]

new_df_q2.sort_values(by='rate', ascending=False, inplace=True)
new_df_q2_1 = new_df_q2.head(100)
st.write(new_df_q2)

df_q2_chart = alt.Chart(new_df_q2_1).mark_area().encode(
    x='title',
    y='price',
    color='rate',
    tooltip=['title', 'price', 'rate']
).interactive()

st.altair_chart(df_q2_chart, theme="streamlit", use_container_width=True)

st.write('### :green[Writer 1 request:]')
# ali to do

st.write('### :green[Test 1:]')
has_not_translate_books_df = pd.read_sql_query(
    'select  price from book where book.id not in (select book.id from book join translator on book.id = translator.book_id)group by book.id;',
    con=mydb)
has_not_translate_books_df['has_trans'] = False
has_translate_books_df = pd.read_sql_query(
    'select  book.id, count(price), price  from book join translator on book.id = translator.book_id group by book.id;',
    con=mydb)
has_translate_books_df['has_trans'] = True
df = pd.concat([has_translate_books_df, has_not_translate_books_df], axis=0)

_, p1_value = ttest_ind(
    has_translate_books_df['price'], has_not_translate_books_df['price'])

alpha = 0.05

if p1_value < alpha:
    print(p1_value,
          "There is a statistically significant difference in prices between has translated book and has not translated book.")
else:
    print(p1_value,
          "There is no statistically significant difference in prices  between has translated book and has not translated book.")

fig, ax = plt.subplots()
sns.boxplot(x='has_trans', y='price', data=df, ax=ax)

st.write('### :green[Test 2:]')
st.write('#### in this test, we are going to study wether there is a noticable difference between the prices of paperbachs and hardcovers.')
st.write('#### since we study difference between two independant samples that are continuous and have a somewhat normal distribution, we have used ttest to verify this assumption:')
with st.container():
    query_p = ("""SELECT b.title, b.cover, b.price
                FROM book b
                WHERE EXISTS (
                    SELECT 1
                    FROM book b1
                    WHERE b1.url_id = b.url_id
                    AND b1.cover = 'شومیز'
                ) AND EXISTS (
                    SELECT 1
                    FROM book b2
                    WHERE b2.url_id = b.url_id
                    AND b2.cover = 'جلد سخت'
                );""")
    df_p = pd.read_sql(query_p, mydb)
    df_shumiz = df_p[df_p['cover'] == 'شومیز']
    df_jeld_sakht = df_p[df_p['cover'] == 'جلد سخت']
    df_shumiz = df_shumiz[df_shumiz['price'] < 10000000]
    df_jeld_sakht = df_jeld_sakht[df_jeld_sakht['price'] < 10000000]
    df_p = df_p[df_p['cover'].isin(['شومیز', 'جلد سخت'])]
    alpha = 0.05
    _, p_value = ttest_ind(df_shumiz['price'], df_jeld_sakht['price'])
    # Check the p-value to determine statistical significance
    alpha = 0.05  # Set the significance level
    st.write('ttest evaluation answer:')
    if p_value < alpha:
        st.write(
            "There is a statistically significant difference in prices between شومیز and جلد سخت.")
    else:
        st.write(
            "There is no statistically significant difference in prices between شومیز and جلد سخت.")
    st.title("Comparison of Prices between شومیز and جلد سخت")

    # Create a boxplot of shumiz covers and jeld_sakht covers
    fig, ax = plt.subplots()
    sns.boxplot(x='cover', y='price', data=df_p, ax=ax)
    ax.set_xlabel("Cover")
    ax.set_ylabel("Price")
    st.pyplot(fig)
    st.title("Distribution Comparison: شومیز vs جلد سخت")

    # Create histograms to see both covers' distributions
    fig, axes = plt.subplots(2, 1, figsize=(10, 10),  sharex=True)
    sns.histplot(df_shumiz['price'], ax=axes[0], kde=True, color='skyblue')
    axes[0].set_title("Distribution of Prices: shumiz covers")
    axes[0].set_xlabel("Price")
    axes[0].set_ylabel("Count")

    sns.histplot(df_jeld_sakht['price'], ax=axes[1],
                 kde=True, color='lightgreen')
    axes[1].set_title("Distribution of Prices: jeld sakht covers")
    axes[1].set_xlabel("Price")
    axes[1].set_ylabel("Count")
    st.pyplot(fig)
    # see the prices of different covers in different ranges:
    fig, axes = plt.subplots(2, 1, figsize=(8, 12), sharex=True)
    # Filter rows with price less than 500000
    df_less_than_500k = df_p[df_p['price'] < 500000]
    # Filter rows with price between 500000 and 5000000
    df_between_500k_5m = df_p[(df_p['price'] >= 500000)]

    # Plot histogram for price less than 500000
    st.title("Price Distribution by Cover Type")
    sns.histplot(df_less_than_500k[df_less_than_500k['cover'] == 'شومیز']
                 ['price'], ax=axes[0], kde=True, color='blue', label='شومیز')
    sns.histplot(df_less_than_500k[df_less_than_500k['cover'] == 'جلد سخت']
                 ['price'], ax=axes[0], kde=True, color='orange', label='جلد سخت')
    axes[0].set_title("Price Distribution: Less than 500,000")
    axes[0].set_ylabel("Count")
    axes[0].legend()

    # Plot histogram for price more than 500000
    sns.histplot(df_between_500k_5m[df_between_500k_5m['cover'] == 'شومیز']
                 ['price'], ax=axes[1], kde=True, color='blue', label='شومیز')
    sns.histplot(df_between_500k_5m[df_between_500k_5m['cover'] == 'جلد سخت']
                 ['price'], ax=axes[1], kde=True, color='orange', label='جلد سخت')
    axes[1].set_title("Price Distribution: 500,000 to 5,000,000")
    axes[1].set_ylabel("Count")
    axes[1].legend()

    st.pyplot(fig)
    st.write(
        "as we observed, hardcovers prices are more seen in the range of (500,000 , )")
