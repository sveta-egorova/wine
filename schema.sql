create table activity
(
	id int
			primary key,
	likes_count int,
	comments_count int
);

create table country
(
	code char(4)
			primary key,
	name varchar(255),
	native_name varchar(255),
	seo_name varchar(255),
	currency_code char(4),
	regions_count int,
	users_count int,
	wines_count int,
	wineries_count int
);

create table food
(
	id int
			primary key,
	name varchar(255),
	seo_name varchar(255)
);

create table grape
(
	id int
			primary key,
	name varchar(255),
	seo_name varchar(255),
	has_detailed_info boolean,
	wines_count int
);

create table grape_country
(
	grape_id int
		references grape,
	country_code char(4)
		references country,
	primary key (grape_id, country_code)
);

create table keyword
(
	id int
			primary key,
	word varchar(255),
	flavor_group varchar(255)
);

create table price
(
	id int
			primary key,
	amount float,
	discounted_from float,
	type varchar(255),
	visibility int,
	currency_code char(4),
	bottle_type varchar(255)
);

create table region
(
	id int
			primary key,
	name varchar(255),
	name_en varchar(255),
	seo_name varchar(255),
	country_code char(4)
		references country
);

create table toplist
(
	id int
			primary key,
	location varchar(255),
	name varchar(255),
	seo_name varchar(255),
	type int,
	year int
);

create table type
(
	id int
			primary key,
	name varchar(255)
);

create table style
(
	id int
			primary key,
	seo_name varchar(255),
	regional_name varchar(255),
	varietal_name varchar(255),
	name varchar(255),
	description varchar(255),
	blurb varchar(255),
	body int,
	body_description varchar(255),
	acidity int,
	acidity_description varchar(255),
	country_code char(4)
		references country,
	type_id int
		references type
);

create table facts
(
	style_id int
		references style,
	fact varchar(255),
    primary key (style_id, fact)

);

create table style_food
(
	style_id int
		references style,
	food_id int
		references food,
	primary key (style_id, food_id)
);

create table style_grape
(
	style_id int
		references style,
	grape_id int
		references grape,
	primary key (style_id, grape_id)
);

create table user
(
	id int
			primary key,
	seo_name varchar(255),
	alias varchar(255),
	is_featured boolean,
	visibility varchar(255),
	followers_count int,
	followings_count int,
	ratings_count int,
	ratings_sum int,
	reviews_count int
);

create table review
(
	id int
			primary key,
	rating float,
	note text,
	language varchar(255),
	created_at datetime,
	aggregated boolean,
	user_id int
		references user,
	activity_id int
		references activity,
	tagged_note varchar(255)
);

create table review_keyword
(
	review_id int
		references review,
	keyword_id int
		references keyword,
	primary key (review_id, keyword_id)
);

create table winery
(
	id int
			primary key,
	name varchar(255),
	seo_name varchar(255),
	status int
);

create table wine
(
	id int
			primary key,
	name varchar(255),
	seo_name varchar(255),
	type_id int
		references type,
	vintage_type int,
	is_natural boolean,
	region_id int
		references region,
	winery_id int
		references winery,
	acidity float,
	fizziness float,
	intensity float,
	sweetness float,
	tannin float,
	user_structure_count int,
	calculated_structure_count int,
	style_id int
		references style,
	status varchar(255),
	ratings_count int,
	ratings_average float,
	labels_count int,
	vintages_count int,
	has_valid_ratings boolean
);

create table vintage
(
	id int
			primary key,
	seo_name varchar(255),
	name varchar(255),
	wine_id int
		references wine,
	year int,
	has_valid_ratings boolean,
	status varchar(255),
	ratings_count int,
	ratings_average float,
	labels_count int,
	price_id int
		references price
);

create table vintage_review
(
	vintage_id int
		references vintage,
	review_id int
		references review,
	primary key (vintage_id, review_id)
);

create table vintage_toplist
(
	vintage_id int
		references vintage,
	toplist_id int
		references toplist,
	`rank` int,
	previous_rank int,
	description varchar(255),
	primary key (vintage_id, toplist_id)
);

create table wine_flavor_group
(
	wine_id int
		references wine,
	flavor_group_name varchar(255)
			references keyword (flavor_group),
	count int,
	score int,
	primary key (wine_id, flavor_group_name)
);

create table wine_keyword
(
	wine_id int
		references wine,
	keyword_id int
		references keyword,
	count int,
	primary key (wine_id, keyword_id)
);
