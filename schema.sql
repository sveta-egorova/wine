-- we don't know how to generate root <with-no-name> (class Root) :(
create table activity
(
	id int
		constraint activity_pk
			primary key,
	likes_count int,
	"comments count" int
);

create table country
(
	code text
		constraint country_pk
			primary key,
	name text,
	native_name text,
	seo_name text,
	currency text,
	regions_count int,
	users_count int,
	wines_count int,
	wineries_count int
);

create table food
(
	id int
		constraint food_pk
			primary key,
	name text,
	seo_name text
);

create table grape
(
	id int
		constraint grapes_pk
			primary key,
	name text,
	seo_name text,
	has_detailed_info boolean,
	wines_count int
);

create table grape_country
(
	grape_id int
		references grape,
	country_code text
		references country,
	constraint grape_country_pk
		unique (grape_id, country_code)
);

create table keyword
(
	id int
		constraint keyword_pk
			primary key,
	word text,
	flavor_group text
);

create table price
(
	id int
		constraint price_pk
			primary key,
	amount float,
	discounted_from float,
	type text
);

create table region
(
	id int
		constraint region_pk
			primary key,
	name text,
	name_en text,
	seo_name text,
	country_code text
		constraint region_country_country_code_fk
			references country
);

create table toplist
(
	id int
		constraint toplist_pk
			primary key,
	location text,
	name text,
	seo_name text,
	type int,
	year int
);

create table type
(
	type_id int
		constraint wine_type_pk
			primary key,
	type_name text
);

create table style
(
	id int
		constraint style_pk
			primary key,
	seo_name text,
	regional_name text,
	varietal_name text,
	name text,
	description text,
	blurb text,
	body int,
	body_description text,
	acidity int,
	acidity_description text,
	country text
		references country,
	wine_type_id int
		references type
);

create table facts
(
	style_id int
		references style,
	fact text
);

create table style_food
(
	style_id int
		references style,
	food_id int
		references food,
	constraint style_food_pk
		unique (style_id, food_id)
);

create table style_grape
(
	style_id int
		references style,
	grape_id int
		references grape,
	constraint style_grape_pk
		unique (style_id, grape_id)
);

create table user
(
	id int
		constraint user_pk
			primary key,
	seo_name text,
	alias text,
	is_featured boolean,
	visibility text,
	followers_count int,
	followings_count int,
	ratings_count int,
	ratings_sum int,
	reviews_count int
);

create table review
(
	id int
		constraint review_pk
			primary key,
	rating float,
	note text,
	language text,
	"created at" text,
	aggregated boolean,
	user_id int
		references user,
	activity_id int
		references activity,
	tagged_note text
);

create table review_keyword
(
	review_id int
		references review,
	keyword_id int
		references keyword,
	constraint review_keyword_pk
		unique (review_id, keyword_id)
);

create table winery
(
	id int
		constraint winery_pk
			primary key,
	name text,
	seo_name text,
	status int
);

create table wine
(
	id int
		constraint wine_pk
			primary key,
	name text,
	seo_name text,
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
	status text,
	ratings_count int,
	ratings_average float,
	labels_count int,
	vintages_count int,
	has_valid_ratings boolean
);

create table vintage
(
	id int
		constraint vintage_pk
			primary key,
	seo_name text,
	name text,
	wine_id int
		constraint vintage_wine_id_fk
			references wine,
	year int,
	has_valid_ratings boolean,
	status text,
	ratings_count int,
	ratings_average float,
	labels_count int
);

create table vintage_price
(
	vintage_id int
		references vintage,
	price_id int
		references price,
	median_amount float,
	type text,
	discounted_from float,
	currency text,
	constraint vintage_price_pk
		unique (vintage_id, price_id)
);

create table vintage_review
(
	vintage_id int
		references vintage,
	review_id int
		references review,
	constraint vintage_review_pk
		unique (vintage_id, review_id)
);

create table vintage_toplist
(
	vintage_id int
		references vintage,
	toplist_id int
		references toplist,
	rank int,
	previous_rank int,
	description text,
	constraint vintage_toplist_pk
		unique (vintage_id, toplist_id)
);

create table wine_flavor_group
(
	wine_id int
		references wine,
	flavor_group_name text
		constraint wine_flavor_group_keyword_flavor_group_fk
			references keyword (flavor_group),
	count int,
	score int,
	constraint wine_flavor_group_pk
		unique (wine_id, flavor_group_name)
);

create table wine_keyword
(
	wine_id int
		references wine,
	keyword_id int
		references keyword,
	count int,
	constraint wine_keyword_pk
		unique (wine_id, keyword_id)
);

