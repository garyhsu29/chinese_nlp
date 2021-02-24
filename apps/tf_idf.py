import pandas as pd
from collections import defaultdict, OrderedDict
import numpy as np

def customize_tfidf(df: pd.DataFrame) -> list:
	idf_denominator_dict = defaultdict(int)
	D = len(df)
	for index, (published_date, news_keywords, counter) in df.iterrows():
		for keyword, count in counter.items():
			idf_denominator_dict[keyword] += 1
	idf_dict = {keyword: np.log(D/count) + 0.0001 for keyword, count in idf_denominator_dict.items()}

	df['td-idf'] = df['count'].apply(lambda x: {keyword: count * idf_dict[keyword] for keyword, count in x.items()})
	#df['td-idf'] = df['td-idf'].apply(lambda x: {word: score for word, score in x})
	#sorted(d.items(), key=lambda x: x[1])
	return df



