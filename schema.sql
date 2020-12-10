create schema vivino collate utf8mb4_unicode_ci;

create table activity
(
	id int not null
		primary key,
	likes_count int null,
	comments_count int null
)
charset=latin1;

create table country
(
	code char(4) not null
		primary key,
	name varchar(255) null,
	native_name varchar(255) null,
	seo_name varchar(255) null,
	currency_code char(4) null,
	regions_count int null,
	users_count int null,
	wines_count int null,
	wineries_count int null
);

create table food
(
	id int not null
		primary key,
	name varchar(255) null,
	seo_name varchar(255) null
);

create table grape
(
	id int not null
		primary key,
	name varchar(255) null,
	seo_name varchar(255) null,
	has_detailed_info tinyint(1) null,
	wines_count int null
);

create table country_grape
(
	country_code char(4) not null,
	grape_id int not null,
	primary key (grape_id, country_code),
	constraint country_grape_country_code_fk
		foreign key (country_code) references country (code),
	constraint grape_country_grape_id_fk
		foreign key (grape_id) references grape (id)
);

create table price
(
	id int not null
		primary key,
	amount float null,
	discounted_from float null,
	type varchar(255) null,
	visibility int null,
	currency_code char(4) null,
	bottle_type varchar(255) null
);

create table region
(
	id int not null
		primary key,
	name varchar(255) null,
	name_en varchar(255) null,
	seo_name varchar(255) null,
	country_code char(4) null,
	constraint region_country_code_fk
		foreign key (country_code) references country (code)
);

create table toplist
(
	id int not null
		primary key,
	location varchar(255) null,
	name varchar(255) null,
	seo_name varchar(255) null,
	type int null,
	year int null
);

create table type
(
	id int not null
		primary key,
	name varchar(255) null
);

create table style
(
	id int not null
		primary key,
	seo_name varchar(255) null,
	regional_name varchar(255) null,
	varietal_name varchar(255) null,
	name varchar(255) null,
	description text null,
	blurb varchar(255) null,
	body int null,
	body_description varchar(255) null,
	acidity int null,
	acidity_description varchar(255) null,
	country_code char(4) null,
	type_id int null,
	constraint style_country_code_fk
		foreign key (country_code) references country (code),
	constraint style_type_id_fk
		foreign key (type_id) references type (id)
);

create table facts
(
	style_id int not null,
	fact text not null,
	constraint facts_style_id_fk
		foreign key (style_id) references style (id)
);

create table style_food
(
	style_id int not null,
	food_id int not null,
	primary key (style_id, food_id),
	constraint style_food_food_id_fk
		foreign key (food_id) references food (id),
	constraint style_food_style_id_fk
		foreign key (style_id) references style (id)
);

create table style_grape
(
	style_id int not null,
	grape_id int not null,
	primary key (style_id, grape_id),
	constraint style_grape_grape_id_fk
		foreign key (grape_id) references grape (id),
	constraint style_grape_style_id_fk
		foreign key (style_id) references style (id)
);

create table user
(
	id int not null
		primary key,
	seo_name varchar(255) null,
	alias varchar(255) null,
	is_featured tinyint(1) null,
	visibility varchar(255) null,
	followers_count int null,
	followings_count int null,
	ratings_count int null,
	ratings_sum int null,
	reviews_count int null
);

create table review
(
	id int not null
		primary key,
	rating float null,
	note mediumtext null,
	language varchar(255) null,
	created_at varchar(255) null,
	aggregated tinyint(1) null,
	user_id int null,
	activity_id int null,
	tagged_note text null,
	constraint review_activity_id_fk
		foreign key (activity_id) references activity (id),
	constraint review_user_id_fk
		foreign key (user_id) references user (id)
);

create table vintage_review
(
	vintage_id int not null,
	review_id int not null,
	primary key (vintage_id, review_id)
)
charset=latin1;

create index vintage_review_review_id_fk
	on vintage_review (review_id);

create table winery
(
	id int not null
		primary key,
	name varchar(255) null,
	seo_name varchar(255) null,
	status int null
);

create table wine
(
	id int not null
		primary key,
	name varchar(255) null,
	seo_name varchar(255) null,
	type_id int null,
	vintage_type int null,
	is_natural tinyint(1) null,
	region_id int null,
	winery_id int null,
	acidity float null,
	fizziness float null,
	intensity float null,
	sweetness float null,
	tannin float null,
	user_structure_count int null,
	calculated_structure_count int null,
	style_id int null,
	status varchar(255) null,
	ratings_count int null,
	ratings_average float null,
	labels_count int null,
	vintages_count int null,
	has_valid_ratings tinyint(1) null,
	constraint wine_region_id_fk
		foreign key (region_id) references region (id),
	constraint wine_style_id_fk
		foreign key (style_id) references style (id),
	constraint wine_type_id_fk
		foreign key (type_id) references type (id),
	constraint wine_winery_id_fk
		foreign key (winery_id) references winery (id)
);

create table vintage
(
	id int not null
		primary key,
	seo_name varchar(255) null,
	name varchar(255) null,
	wine_id int null,
	year int null,
	has_valid_ratings tinyint(1) null,
	status varchar(255) null,
	ratings_count int null,
	ratings_average float null,
	labels_count int null,
	price_id int null,
	price float null,
	constraint vintage_price_id_fk
		foreign key (price_id) references price (id),
	constraint vintage_wine_id_fk
		foreign key (wine_id) references wine (id)
);

create table vintage_toplist
(
	vintage_id int not null,
	toplist_id int not null,
	`rank` int null,
	previous_rank int null,
	description varchar(255) null,
	primary key (vintage_id, toplist_id),
	constraint vintage_toplist_toplist_id_fk
		foreign key (toplist_id) references toplist (id),
	constraint vintage_toplist_vintage_id_fk
		foreign key (vintage_id) references vintage (id)
);

