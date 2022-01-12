import streamlit as st
import os, sys
import plotly.graph_objects as go
from datetime import timedelta, datetime 
import pandas as pd
from collections import Counter
cur_dir_name = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.dirname(cur_dir_name)
sys.path.append(parent_dir_name)
from db_func import query_from_db
from wordcloud import WordCloud
from tf_idf import customize_tfidf
import pytz
from datetime import datetime
import time


font = os.path.join(cur_dir_name, 'SourceHanSansTW-Regular.otf')


@st.cache()
def get_rss_data():
	count_df = query_from_db("""
	SELECT DATE(nrf.created_at) AS DATE, nrf.rss_source, COUNT(*) AS COUNT
	FROM news_db.news_rss_feeds nrf
	GROUP BY DATE(nrf.created_at), nrf.rss_source ORDER BY DATE(nrf.created_at);""")
	return count_df

@st.cache()
def get_success_parse_data():
	success_df = query_from_db("""
		SELECT DATE(nrf.updated_at) AS DATE, nrf.rss_source, COUNT(*) AS COUNT
		FROM news_db.news_rss_feeds nrf
		WHERE nrf.processed_status = 1 and nrf.processed_success = 1
		GROUP BY DATE(nrf.updated_at), nrf.rss_source ORDER BY DATE(nrf.updated_at);""")
	return success_df
@st.cache()
def get_fail_parse_data():
	fail_df = query_from_db("""
		SELECT DATE(nrf.updated_at) AS DATE, nrf.rss_source, COUNT(*) AS COUNT
		FROM news_db.news_rss_feeds nrf
		WHERE nrf.processed_status = 1 and nrf.processed_success = 0
		GROUP BY DATE(nrf.updated_at), nrf.rss_source ORDER BY DATE(nrf.updated_at);""")
	return fail_df

@st.cache(allow_output_mutation=True)
def get_keywords_by_date():
	kw_df = query_from_db("""SELECT published_date, news_keywords 
		FROM news_db.news_contents
		WHERE news_keywords is not NULL and news_keywords != '' and published_date > '2022-01-01';""")
	return kw_df

@st.cache(allow_output_mutation = True)
def get_company_overview(selected_date):
	company_df = query_from_db("""SELECT nkv.ent_text as company_name, COUNT(*) as count
		FROM news_db.news_kw_view  nkv
		WHERE EXISTS (SELECT 1 FROM news_db.stock_companies ysc WHERE nkv.ent_text = ysc.company_name) and ent_type = 'ORG' and published_date = '{}'
		GROUP BY ent_text""".format(selected_date))
	return company_df

def get_ner_by_date(selected_date):
	ner_df = query_from_db("""SELECT nkv.published_date, nkv.ent_text, nkv.ent_type 
		FROM news_db.news_kw_view nkv
		WHERE nkv.published_date  = '{}';""".format(selected_date))

	# ner_df = query_from_db("""(SELECT 
	# 	`nn`.`ent_text` AS `ent_text`,
	# 	`nn`.`ent_type` AS `ent_type`,
	# 	`nc`.`published_date` AS `published_date`
	# FROM
	# 	((`news_db`.`news_ners` `nn`
	# 	JOIN `news_db`.`news_sents` `ns` ON ((`ns`.`news_sent_id` = `nn`.`news_sent_id`)))
	# 	JOIN `news_db`.`news_contents` `nc` ON ((`nc`.`news_id` = `ns`.`news_id`)))
	# WHERE
	# 	(`nn`.`ent_type` IN ('ORG' , 'PERSON') AND nc.published_date = '{}')) """.format(selected_date))
	return ner_df

def get_top_news(selected_date):
	top_news_df = query_from_db("""SELECT aftv.news_id, aftv.news_title, nts.news_cluster, nrf.news_url FROM news_db.all_financial_title_view aftv
	JOIN news_db.news_title_selection nts on nts.news_id = aftv.news_id
	JOIN news_db.news_contents nc on nc.news_id = aftv.news_id
	JOIN news_db.news_rss_feeds nrf on nrf.news_rss_feeds_id = nc.news_rss_feeds_id
	WHERE aftv.published_date = '{}'""".format(str(selected_date)))
	return top_news_df
if __name__ == '__main__':
	module = st.sidebar.selectbox('Mode: ', ['Crawling DB analysis', 'News Analysis (Basic Count)', 'New Analysis (TF-IDF)', 'New Analysis (NER Count)', 'News Analysis (companies)', 'Top News'])
	if module == 'Crawling DB analysis':
		mode = st.sidebar.selectbox('Mode: ', ['Overview (Parse All)', 'Overview (Raw RSS)', 
			'Detailed (Raw RSS)', 'Overview (Parse Success)', 'Datailed (Parse Success)', 'Overview (Parse Failed)', 'Detailed (Parse Failed)'])
		count_df = get_rss_data()
		success_df = get_success_parse_data()
		fail_df = get_fail_parse_data()
		if mode == 'Overview (Raw RSS)':
			st.title("How many rss feed per day?")
			sub_mode = st.selectbox('Plot mode: ', ['Bar Chart', 'Line Chart', 'Bar Line Chart'])
			ov_df = count_df[['DATE', 'COUNT']].groupby(['DATE']).agg(sum)
			if sub_mode == 'Bar Chart':
				fig = go.Figure([go.Bar(x = ov_df.index, y = ov_df['COUNT'])])
				st.write(fig)
			elif sub_mode == 'Line Chart':
				fig = go.Figure(data=go.Scatter(x = ov_df.index, y = ov_df['COUNT']))
				st.write(fig)
			elif sub_mode == 'Bar Line Chart':
				fig = go.Figure()
				fig.add_trace(
					go.Scatter(name = None, x = ov_df.index, y = ov_df['COUNT']))
				fig.add_trace(
					go.Bar(x = ov_df.index, y = ov_df['COUNT']))
				fig.update_layout(showlegend=False)
				st.write(fig)
		elif mode == 'Detailed (Raw RSS)':
			st.title("How many rss feed per day?")
			last_date = max(count_df['DATE'])
			first_date = min(count_df['DATE'])
			start_date = st.date_input('Start Date: ', last_date - timedelta(days=3), min_value = first_date  , max_value = last_date)
			end_date = st.date_input('End Date: ', last_date, min_value = first_date, max_value = last_date)
			new_df = count_df[(count_df['DATE'] >= start_date) & (count_df['DATE'] <= end_date)]
			top_5_source = new_df.groupby(['rss_source']).agg(sum).sort_values(by = ['COUNT']).index[-5:].values
			#st.write()
			selected_source = st.multiselect('The source you are interested in (Default is top 5 count source):', new_df['rss_source'].unique().tolist(),top_5_source)
			fig = go.Figure(data=[ go.Bar(name = cat, x = new_df[new_df['rss_source'] == cat]['DATE'], y = new_df[new_df['rss_source'] == cat]['COUNT']) 
				for cat in count_df['rss_source'].unique() if cat in selected_source])
			# Change the bar mode
			fig.update_layout(barmode='group')
			st.write(fig)
		elif mode == 'Overview (Parse Success)':
			st.title("How many rss feed parsed successfully per day?")
			sub_mode = st.selectbox('Plot mode: ', ['Bar Chart', 'Line Chart', 'Bar Line Chart'])
			ov_df = success_df[['DATE', 'COUNT']].groupby(['DATE']).agg(sum)
			if sub_mode == 'Bar Chart':
				fig = go.Figure([go.Bar(x = ov_df.index, y = ov_df['COUNT'])])
				st.write(fig)
			elif sub_mode == 'Line Chart':
				fig = go.Figure(data=go.Scatter(x = ov_df.index, y = ov_df['COUNT']))
				st.write(fig)
			elif sub_mode == 'Bar Line Chart':
				fig = go.Figure()
				fig.add_trace(
					go.Scatter(name = None, x = ov_df.index, y = ov_df['COUNT']))
				fig.add_trace(
					go.Bar(x = ov_df.index, y = ov_df['COUNT']))
				fig.update_layout(showlegend=False)
				st.write(fig)

		elif mode == 'Datailed (Parse Success)':
			st.title("How many rss feed parsed successfully per day?")
			last_date = max(success_df['DATE'])
			first_date = min(success_df['DATE'])
			start_date = st.date_input('Start Date: ', last_date - timedelta(days=3), min_value = first_date  , max_value = last_date)
			end_date = st.date_input('End Date: ', last_date, min_value = first_date, max_value = last_date)
			new_df = success_df[(success_df['DATE'] >= start_date) & (success_df['DATE'] <= end_date)]
			top_5_source = new_df.groupby(['rss_source']).agg(sum).sort_values(by = ['COUNT']).index[-5:].values
			selected_source = st.multiselect('The source you are interested in (Default is top 5 count source):', new_df['rss_source'].unique().tolist(),top_5_source)
			fig = go.Figure(data=[ go.Bar(name = cat, x = new_df[new_df['rss_source'] == cat]['DATE'], y = new_df[new_df['rss_source'] == cat]['COUNT']) 
				for cat in success_df['rss_source'].unique() if cat in selected_source])
			# Change the bar mode
			fig.update_layout(barmode='group')
			st.write(fig)

		elif mode == 'Overview (Parse Failed)':
			st.title("How many rss feed parsed wrongly per day?")
			sub_mode = st.selectbox('Plot mode: ', ['Bar Chart', 'Line Chart', 'Bar Line Chart'])
			ov_df = fail_df[['DATE', 'COUNT']].groupby(['DATE']).agg(sum)
			if sub_mode == 'Bar Chart':
				fig = go.Figure([go.Bar(x = ov_df.index, y = ov_df['COUNT'])])
				st.write(fig)
			elif sub_mode == 'Line Chart':
				fig = go.Figure(data=go.Scatter(x = ov_df.index, y = ov_df['COUNT']))
				st.write(fig)
			elif sub_mode == 'Bar Line Chart':
				fig = go.Figure()
				fig.add_trace(
					go.Scatter(name = None, x = ov_df.index, y = ov_df['COUNT']))
				fig.add_trace(
					go.Bar(x = ov_df.index, y = ov_df['COUNT']))
				fig.update_layout(showlegend=False)
				st.write(fig)

		elif mode == 'Detailed (Parse Failed)':
			st.title("How many rss feed parsed wrongly per day?")
			last_date = max(fail_df['DATE'])
			first_date = min(fail_df['DATE'])
			start_date = st.date_input('Start Date: ', last_date - timedelta(days=3), min_value = first_date  , max_value = last_date)
			end_date = st.date_input('End Date: ', last_date, min_value = first_date, max_value = last_date)
			new_df = fail_df[(fail_df['DATE'] >= start_date) & (fail_df['DATE'] <= end_date)]
			top_5_source = new_df.groupby(['rss_source']).agg(sum).sort_values(by = ['COUNT']).index[-5:].values
			selected_source = st.multiselect('The source you are interested in (Default is top 5 count source):', new_df['rss_source'].unique().tolist(),top_5_source)
			fig = go.Figure(data=[ go.Bar(name = cat, x = new_df[new_df['rss_source'] == cat]['DATE'], y = new_df[new_df['rss_source'] == cat]['COUNT']) 
				for cat in fail_df['rss_source'].unique() if cat in selected_source])
			# Change the bar mode
			fig.update_layout(barmode='group')
			st.write(fig)
		elif mode == 'Overview (Parse All)':
			st.title("What is the ratio that rss feed parsed wrongly per day?")
			success_df_copy = success_df.rename(columns={"COUNT": "success_count"})
			fail_df_copy = fail_df.rename(columns={"COUNT": "fail_count"})
			all_df = pd.merge(success_df_copy, fail_df_copy, on=["DATE", "rss_source"], how = 'outer')
			all_df.fillna(0, inplace = True)
			all_df['all_count'] = all_df.apply(lambda x: x['success_count'] + x['fail_count'], axis = 1)
			last_date = max(all_df['DATE'])
			first_date = min(all_df['DATE'])
			start_date = st.date_input('Start Date: ', last_date - timedelta(days=3), min_value = first_date  , max_value = last_date)
			end_date = st.date_input('End Date: ', last_date, min_value = first_date, max_value = last_date)
			new_df = all_df[(all_df['DATE'] >= start_date) & (all_df['DATE'] <= end_date)]

			top_5_source = new_df.groupby(['rss_source']).agg(sum).sort_values(by = ['all_count']).index[-5:].values
			selected_source = st.multiselect('The source you are interested in (Default is top 5 count source):', new_df['rss_source'].unique().tolist(),top_5_source)
			#st.text([(([new_df[new_df['category_name'] == cat]['DATE'], new_df[new_df['category_name'] == cat]['category_name']]), new_df[new_df['category_name'] == cat][count])  for count in ('success_count', 'fail_count') for cat in count_df['category_name'].unique() if cat in selected_source])
			fig = go.Figure(data=[ go.Bar(x = [new_df[new_df['rss_source'] == cat]['DATE'], new_df[new_df['rss_source'] == cat]['rss_source']], y = new_df[new_df['rss_source'] == cat][count], name = count)  for count in ('success_count', 'fail_count') for cat in count_df['rss_source'].unique() if cat in selected_source])
			# Change the bar mode
			fig.update_layout(barmode="stack")
			st.write(fig)
	elif module == 'News Analysis (Basic Count)':
		kw_df = get_keywords_by_date()
		st.title("Word of a Day")
		last_date = max(kw_df['published_date'])
		first_date = datetime.strptime("2022-01-01", "%Y-%m-%d")
		selected_date = st.date_input('Select date: ', last_date, min_value = first_date  , max_value = last_date)
		stop_kw_list = set(['自由時報', '自由時報電子報', '大紀元', 'ltn', '自由娛樂', 'LTN', 'Liberty Times Net', 'nownews'])
		
		temp = kw_df.groupby(['published_date'])['news_keywords'].apply(', '.join).reset_index()
		temp = temp[temp['published_date'] == selected_date]
		temp['news_keywords'] = temp['news_keywords'].apply(lambda x: [str(word.strip()) for word in x.split(',') if word.strip() not in stop_kw_list])
		temp['count'] = temp['news_keywords'].apply(lambda x: Counter(x))
		#temp['count'] = temp['news_keywords'].apply(Counter)
		#st.write(type(temp[temp['published_date'] == selected_date]['news_keywords'].values[:2]))
		#st.text(temp[temp['published_date'] == selected_date]['news_keywords'].values[0])
		#maximum_count = len(temp.iloc[0]['count'])
		topn = st.slider('Most Common Words', 0, 50, 10, 1)
		my_wordcloud = WordCloud(max_words = topn, background_color='white',font_path=font, width=700, height=300)
		my_wordcloud.generate_from_frequencies(temp.iloc[0]['count'])
		image = my_wordcloud.to_image()
		st.image(image)
		
		counter = temp.iloc[0]['count'].most_common(topn)

		words, count = zip(*counter)
		fig = go.Figure([go.Bar(x = count[::-1], y = words[::-1], orientation='h')])
		fig.update_layout(font_size = 12)
		st.write(fig)
	elif module == 'New Analysis (TF-IDF)':
		#stop_kw_list = set(['自由時報', '自由時報電子報', '大紀元', 'ltn', '自由娛樂', 'LTN', 'Liberty Times Net', 'nownews'])
		kw_df = get_keywords_by_date()
		st.title("Word of a Day")
		last_date = max(kw_df['published_date'])
		first_date = datetime.strptime("2022-01-01", "%Y-%m-%d")
		selected_date = st.date_input('Select date: ', last_date, min_value = first_date  , max_value = last_date)
		stop_kw_list = set(['自由時報', '自由時報電子報', '大紀元', 'ltn', '自由娛樂', 'LTN', 'Liberty Times Net', 'nownews'])
		
		temp = kw_df.groupby(['published_date'])['news_keywords'].apply(', '.join).reset_index()
		temp['news_keywords'] = temp['news_keywords'].apply(lambda x: [str(word.strip()) for word in x.split(',') if word.strip() not in stop_kw_list])
		temp['count'] = temp['news_keywords'].apply(lambda x: Counter(x))
		customize_tfidf(temp)
		temp = temp[temp['published_date'] == selected_date]
		topn = st.slider('Most Common Words', 0, 50, 10, 1)
		my_wordcloud = WordCloud(max_words = topn, background_color='white',font_path=font, width=700, height=300)
		my_wordcloud.generate_from_frequencies(temp.iloc[0]['td-idf'])
		image = my_wordcloud.to_image()
		st.image(image)
		#st.write(type(temp.iloc[0]['td-idf']))
		counter = [(key, score) for key, score in temp.iloc[0]['td-idf'].items()]
		counter = sorted(counter, key = lambda x: x[1], reverse = True)
		#st.write(counter[:10])
		#print(counter[:10])
		counter = counter[:topn]
		words, count = zip(*counter)
		fig = go.Figure([go.Bar(x = count[::-1], y = words[::-1], orientation='h')])
		fig.update_layout(font_size = 12)
		st.write(fig)
	elif module == 'New Analysis (NER Count)':
		end_date = datetime.now(tz = pytz.timezone('Asia/Taipei')).date()
		start_date = datetime.strptime("2021-01-19", "%Y-%m-%d")
		selected_date = st.date_input('Select date: ', end_date, min_value = start_date  , max_value = end_date)
		ent_type_raw = st.selectbox('Entity Type: ', ['Person', "Organization"])
		ent_type_dict = {'Person': 'PERSON', 'Organization':"ORG"}
		ent_type = ent_type_dict[ent_type_raw]
		ner_df = get_ner_by_date(selected_date)
		ner_df = ner_df.drop(columns = ['published_date'])
		ner_df = ner_df[ner_df['ent_type'] == ent_type].groupby(['ent_text']).agg('count')['ent_type'].sort_values(ascending = False)
		topn = st.slider('Most Common Words', 0, 50, 10, 1)
		my_wordcloud = WordCloud(max_words = topn, background_color='white',font_path=font, width=700, height=300)
		#st.write(ner_df)
		my_wordcloud.generate_from_frequencies(ner_df)
		image = my_wordcloud.to_image()
		st.image(image)
		words = ner_df.index[:topn]
		counts = ner_df.values[:topn]
		fig = go.Figure([go.Bar(x = counts[::-1], y = words[::-1], orientation='h')])
		fig.update_layout(font_size = 12)
		st.write(fig)

	elif module == 'News Analysis (companies)':
		end_date = datetime.now(tz = pytz.timezone('Asia/Taipei')).date()
		start_date = datetime.strptime("2021-01-19", "%Y-%m-%d").date()
		selected_date = st.date_input('Select date: ', end_date, min_value = start_date  , max_value = end_date)
		company_df = get_company_overview(selected_date)
		# company_df = company_df[company_df['published_date'] == selected_date]
		# company_df = company_df.drop(columns = ['published_date'])
		company_df = company_df.set_index('company_name')
		company_df = company_df['count'].astype(int)
		company_df = company_df.sort_values(ascending = False)
		topn = st.slider('Most Common Words', 0, 50, 10, 1)
		my_wordcloud = WordCloud(max_words = topn, background_color='white',font_path=font, width=700, height=300)
		my_wordcloud.generate_from_frequencies(company_df)
		image = my_wordcloud.to_image()
		st.image(image)
		words = company_df.index[:topn]
		counts = company_df.values[:topn]
		fig = go.Figure([go.Bar(x = counts[::-1], y = words[::-1], orientation='h')])
		fig.update_layout(font_size = 12)
		st.write(fig)
	elif module == 'Top News':
		end_date = datetime.now(tz = pytz.timezone('Asia/Taipei')).date() - timedelta(days = 3)
		start_date = datetime.strptime("2021-01-19", "%Y-%m-%d").date()
		selected_date = st.date_input('Select date: ', end_date, min_value = start_date  , max_value = end_date)
		df = get_top_news(selected_date)

		topics = st.selectbox('Topics: ', df['news_cluster'].unique())
		st.write(df.loc[df['news_cluster'] == topics])

		
