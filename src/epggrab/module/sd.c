#define _GNU_SOURCE

#include <stdio.h>
#include <malloc.h>
#include <string.h>
#include <openssl/sha.h>
#include <curl/curl.h>
#include <time.h>

#include "htsmsg_json.h"
#include "settings.h"

#include "tvheadend.h"
#include "channels.h"
#include "spawn.h"
#include "htsstr.h"

#include "lang_str.h"
#include "epg.h"
#include "epggrab.h"
#include "epggrab/private.h"

static epggrab_channel_tree_t	_sd_channels;

typedef struct epggrab_module_sd
{
	epggrab_module_int_t mod;
	char username[64];
	char sha1_password[SHA_DIGEST_LENGTH*2+1];
	int flush;
	time_t update;
} epggrab_module_sd_t;

struct buffer
{
	int cur_size;
	int max_size;
	char *ptr;
};

static size_t write_callback(void *contents, size_t size, size_t nmemb, void *userp)
{
	struct buffer *buf = (struct buffer *)userp;
	int offset = buf->cur_size;
	buf->cur_size += (size * nmemb);
	if (buf->cur_size > buf->max_size)
	{
		buf->max_size += (buf->cur_size + 4095) & ~4095;
		buf->ptr = realloc(buf->ptr, buf->max_size);
	}
	memcpy(buf->ptr+offset, contents, (size * nmemb));
	return (size * nmemb);
}

struct program_buffer
{
	int cur_size;
	int max_size;
	char *ptr;
	htsmsg_t *m;
	const char *id;
};

static size_t program_callback(void *contents, size_t size, size_t nmemb, void *userp)
{
	htsmsg_t *m;
	const char *id;
	int i, len = size * nmemb;
	struct program_buffer *buf = (struct program_buffer *)userp;

	int offset = buf->cur_size;

	if (buf->cur_size + len > buf->max_size)
	{
		buf->max_size += (buf->cur_size + len + 4095) & ~4095;
		buf->ptr = realloc(buf->ptr, buf->max_size);
	}

	for (i=0; i<len; i++)
	{
		if (((char *)contents)[i] == '\n' || ((char *)contents)[i] == '\r')
		{
			if (offset > 0)
			{
				buf->ptr[offset] = '\0';
				m = htsmsg_json_deserialize(buf->ptr);
				if (m)
				{
					id = htsmsg_get_str(m, buf->id);
					if (id)
						htsmsg_add_msg(buf->m, id, m);
					else
						printf("program_callback id error: %s\n", buf->ptr);
				}
				else
					printf("program_callback m error: %s\n", buf->ptr);
			}

			offset = 0;
		}
		else
		{
			buf->ptr[offset] = ((char *)contents)[i];
			offset ++;
		}
	}
	buf->cur_size = offset;

	return len;
}

static int get_token(CURL *curl, char *username, char *sha1_hex, char *token)
{
	char *out;
	const char *ptr;
	int code = -1;
	struct buffer buf = { 0, 0, NULL };
	htsmsg_t *m;

	m = htsmsg_create_map();
	htsmsg_add_str(m, "username", username);
	htsmsg_add_str(m, "password", sha1_hex);
	out = htsmsg_json_serialize_to_str(m, 0);
	htsmsg_destroy(m);

	curl_easy_setopt(curl, CURLOPT_URL, "https://json.schedulesdirect.org/20140530/token");
	curl_easy_setopt(curl, CURLOPT_POST, 1L);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, out);
#if 0
	curl_easy_setopt(curl, CURLOPT_VERBOSE, 1);
#endif
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

	curl_easy_perform(curl);

	free(out);

	buf.ptr[buf.cur_size] = '\0';
	m = htsmsg_json_deserialize(buf.ptr);
	if (m)
	{
		if (!htsmsg_get_s32(m, "code", &code) && code == 0)
		{
			if ((ptr = htsmsg_get_str(m, "token")))
				strcpy(token, ptr);
		}
		htsmsg_destroy(m);
	}
	else
		printf("get_token error: %s\n", buf.ptr);

	free(buf.ptr);
	return code;
}

static htsmsg_t *add_lineup(CURL *curl, const char *uri)
{
	char url[160];
	int code = -1;
	struct buffer buf = { 0, 0, NULL };
	htsmsg_t *m;

	snprintf(url, sizeof(url), "https://json.schedulesdirect.org%s", uri);

	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
	curl_easy_setopt(curl, CURLOPT_INFILESIZE, 0L);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

	curl_easy_perform(curl);

	if (buf.cur_size > 0)
	{
		buf.ptr[buf.cur_size] = '\0';
		m = htsmsg_json_deserialize(buf.ptr);
		if (m)
		{
			if (htsmsg_get_s32(m, "code", &code) || code != 0)
			{
				printf("add_lineup code error: %s\n", buf.ptr);
				htsmsg_destroy(m);
				m = NULL;
			}
		}
		else
			printf("add_lineup m error: %s\n", buf.ptr);
	}
	free(buf.ptr);
	return m;
}

static htsmsg_t *get_status(CURL *curl)
{
	int code = -1;
	struct buffer buf = { 0, 0, NULL };
	htsmsg_t *m = NULL;

	curl_easy_setopt(curl, CURLOPT_URL, "https://json.schedulesdirect.org/20140530/status");
	curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

	curl_easy_perform(curl);

	if (buf.cur_size > 0)
	{
		buf.ptr[buf.cur_size] = '\0';
		m = htsmsg_json_deserialize(buf.ptr);
		if (m)
		{
			if (htsmsg_get_s32(m, "code", &code) || code != 0)
			{
				printf("get_status code error: %s\n", buf.ptr);
				htsmsg_destroy(m);
				m = NULL;
			}
		}
		else
			printf("get_status m error: %s\n", buf.ptr);
	}
	free(buf.ptr);
	return m;
}

static htsmsg_t *get_lineups(CURL *curl)
{
	int code = -1;
	struct buffer buf = { 0, 0, NULL };
	htsmsg_t *m = NULL;

	curl_easy_setopt(curl, CURLOPT_URL, "https://json.schedulesdirect.org/20140530/lineups");
	curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

	curl_easy_perform(curl);

	if (buf.cur_size > 0)
	{
		buf.ptr[buf.cur_size] = '\0';
		m = htsmsg_json_deserialize(buf.ptr);
		if (m)
		{
			if (!htsmsg_get_s32(m, "code", &code) || code != 0)
			{
				printf("get_lineups code error: %s\n", buf.ptr);
				htsmsg_destroy(m);
				m = NULL;
			}
		}
		else
			printf("get_lineups m error: %s\n", buf.ptr);
	}
	free(buf.ptr);
	return m;
}

static htsmsg_t *get_stations(CURL *curl, const char *uri)
{
	struct buffer buf = { 0, 0, NULL };
	char url[160];
	htsmsg_t *m;

	snprintf(url, sizeof(url), "https://json.schedulesdirect.org%s", uri);

	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

	curl_easy_perform(curl);

	buf.ptr[buf.cur_size] = '\0';
	m = htsmsg_json_deserialize(buf.ptr);
	if (m == NULL)
	{
		printf("get_stations(%s) error: len=%d, ptr=%s\n", uri, buf.cur_size, buf.ptr);
	}
	free(buf.ptr);

	return m;
}

static htsmsg_t *get_schedules_md5(CURL *curl, htsmsg_t *l)
{
	char *out;
	struct buffer buf = { 0, 0, NULL };
	htsmsg_t *m;

	out = htsmsg_json_serialize_to_str(l, 0);
	htsmsg_destroy(l);

	curl_easy_setopt(curl, CURLOPT_URL, "https://json.schedulesdirect.org/20140530/schedules/md5");
	curl_easy_setopt(curl, CURLOPT_POST, 1L);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, out);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

	curl_easy_perform(curl);

	buf.ptr[buf.cur_size] = '\0';
	m = htsmsg_json_deserialize(buf.ptr);
	if (m == NULL)
	{
		printf("get_schedules_md5() error: len=%d, ptr=%s\n", buf.cur_size, buf.ptr);
	}
	free(buf.ptr);

	return m;
}

static htsmsg_t *get_schedules(CURL *curl, htsmsg_t *l)
{
	char *out;
	struct program_buffer buf = { 0, 0, NULL, htsmsg_create_map(), "stationID" };
	htsmsg_t *m;
	const char *id;

	out = htsmsg_json_serialize_to_str(l, 0);
	htsmsg_destroy(l);

	curl_easy_setopt(curl, CURLOPT_URL, "https://json.schedulesdirect.org/20140530/schedules");
	curl_easy_setopt(curl, CURLOPT_POST, 1L);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, out);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, program_callback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

	curl_easy_perform(curl);

	free(out);

	if (buf.cur_size > 0)
	{
		buf.ptr[buf.cur_size] = '\0';
		m = htsmsg_json_deserialize(buf.ptr);
		if (m)
		{
			id = htsmsg_get_str(m, buf.id);
			if (id)
				htsmsg_add_msg(buf.m, id, m);
			else
				printf("get_schedules id error: %s\n", buf.ptr);
		}
		else
			printf("get_schedules m error: %s\n", buf.ptr);

	}
	free(buf.ptr);

	return buf.m;
}

static htsmsg_t *get_episodes(CURL *curl, htsmsg_t *map, htsmsg_t *l)
{
	char *out;
	struct program_buffer buf = { 0, 0, NULL, map, "programID" };
	htsmsg_t *m;
	const  char *id;

	out = htsmsg_json_serialize_to_str(l, 0);
	htsmsg_destroy(l);

	curl_easy_setopt(curl, CURLOPT_URL, "https://json.schedulesdirect.org/20140530/programs");
	curl_easy_setopt(curl, CURLOPT_POST, 1L);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, out);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, program_callback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

	curl_easy_perform(curl);

	free(out);

	if (buf.cur_size > 0)
	{
		buf.ptr[buf.cur_size] = '\0';
		m = htsmsg_json_deserialize(buf.ptr);
		if (m)
		{
			id = htsmsg_get_str(m, buf.id);
			if (id)
				htsmsg_add_msg(buf.m, id, m);
			else
				printf("get_episodes id error: %s\n", buf.ptr);
		}
		else
			printf("get_episodes m error: %s\n", buf.ptr);
	}
	free(buf.ptr);

	return buf.m;
}

static time_t _sp_str2time(const char *in)
{
	struct tm tm;

	memset(&tm, 0, sizeof(tm));

	strptime(in, "%FT%T%z", &tm);

	return timegm(&tm);
}

static time_t _sp_str2date(const char *in)
{
	struct tm tm;

	memset(&tm, 0, sizeof(tm));

	strptime(in, "%F", &tm);

	return timegm(&tm);
}

static int sd_parse_titles(
	void *mod,
	epg_episode_t *ee,
	htsmsg_t *episode)
{
	int save = 0;
	const char *title, *subtitle;
	htsmsg_t *m, *titles;
	htsmsg_field_t *f;

	titles = htsmsg_get_list(episode, "titles");

	if (titles)
	{
		HTSMSG_FOREACH(f, titles)
		{
	                m = htsmsg_get_map_by_field(f);
			title = htsmsg_get_str(m, "title120");

			if (title)
			{
				save |= epg_episode_set_title(ee, title, "en", mod);
			}
		}
	}

	subtitle = htsmsg_get_str(episode, "episodeTitle150");

	if (subtitle)
	{
		save |= epg_episode_set_subtitle(ee, subtitle, "en", mod);
	}

	return save;
}

static int sd_parse_metadata(
	void *mod,
	epg_episode_t *ee,
	epg_episode_num_t *epnum,
	htsmsg_t *episode)
{
	int save = 0;
	htsmsg_t *metadata, *m, *provider;
	htsmsg_field_t *f, *g;
	int tmp;

	metadata = htsmsg_get_list(episode, "metadata");
	if (metadata)
	{
	        HTSMSG_FOREACH(f, metadata)
	        {
	                m = htsmsg_get_map_by_field(f);
			HTSMSG_FOREACH(g, m)
			{
				provider = htsmsg_get_map_by_field(g);
				if (strcmp(g->hmf_name, "Tribune"))
					printf("Provider: %s\n", g->hmf_name);

				htsmsg_get_s32(provider, "season", &tmp);
				epnum->s_num = tmp;
				htsmsg_get_s32(provider, "episode", &tmp);
				epnum->e_num = tmp;

				save |= epg_episode_set_epnum(ee, epnum, mod);
			}
	        }
	}

	return save;
}

static int sd_parse_description(
	void *mod,
	epg_broadcast_t *ebc,
	htsmsg_t *episode)
{
	int save = 0;
	htsmsg_t *descriptions, *m, *desc1;
	htsmsg_field_t *f;
	const char *desc, *lang;

	descriptions = htsmsg_get_map(episode, "descriptions");
	if (descriptions)
	{
		desc1 = htsmsg_get_list(descriptions, "description1000");
		if (desc1)
		{
			HTSMSG_FOREACH(f, desc1)
			{
				m = htsmsg_get_map_by_field(f);

				lang = htsmsg_get_str(m, "descriptionLanguage");
				desc = htsmsg_get_str(m, "description");

				save |= epg_broadcast_set_description(ebc, desc, lang, mod);
			}
		}

		desc1 = htsmsg_get_list(descriptions, "description100");
		if (desc1)
		{
			HTSMSG_FOREACH(f, desc1)
			{
				m = htsmsg_get_map_by_field(f);

				lang = htsmsg_get_str(m, "descriptionLanguage");
				desc = htsmsg_get_str(m, "description");

				save |= epg_broadcast_set_summary(ebc, desc, lang, mod);
			}
		}
	}

	return save;
}

static int sd_parse_genre(
	void *mod,
	epg_episode_t *ee,
	htsmsg_t *episode)
{
	int save = 0;
	htsmsg_t *genre;
	htsmsg_field_t *f;
	const char *g;
	epg_genre_list_t *genres = NULL;
	memset(&genres, 0x00, sizeof(epg_genre_list_t));

	genre = htsmsg_get_list(episode, "genres");
	if (genre)
	{
		HTSMSG_FOREACH(f, genre)
		{
			g = htsmsg_field_get_str(f);
			if (g)
			{
				if (!genres)
					genres = calloc(1, sizeof(epg_genre_list_t));
				epg_genre_list_add_by_str(genres, g);
			}
		}

		if (genres)
		{
			save |= epg_episode_set_genre(ee, genres, mod);
			epg_genre_list_destroy(genres);
		}
	}

	return save;
}

static int sd_parse_content_rating(
	void *mod,
	epg_episode_t *ee,
	htsmsg_t *episode)
{
	int save = 0;
	htsmsg_t *m, *rating;
	htsmsg_field_t *f;
	const char *body, *code;
	int age;

	rating = htsmsg_get_list(episode, "contentRating");
	if (rating)
	{
		HTSMSG_FOREACH(f, rating)
		{
	                m = htsmsg_get_map_by_field(f);

			body = htsmsg_get_str(m, "body");
			code = htsmsg_get_str(m, "code");

			if (strcmp(code, "G") == 0)
				age = 0;
			else if (strcmp(code, "U") == 0)
				age = 0;
			else if (strcmp(code, "A") == 0)
				age = 0;
			else if (strcmp(code, "L") == 0)
				age = 0;
			else if (strcmp(code, "Family") == 0)
				age = 0;
			else if (strcmp(code, "TVG") == 0)
				age = 0;
			else if (strcmp(code, "TVY") == 0)
				age = 0;
			else if (strcmp(body, "Canadian Parental Rating") == 0 && strcmp(code, "C") == 0)
				age = 1;
			else if (strcmp(code, "TVY7") == 0)
				age = 7;
			else if (strcmp(code, "PG") == 0)
				age = 10;
			else if (strcmp(code, "TVPG") == 0)
				age = 10;
			else if (strcmp(code, "B") == 0)
				age = 12;
			else if (strcmp(code, "PG-13") == 0)
				age = 13;
			else if (strcmp(code, "TV14") == 0)
				age = 14;
			else if (strcmp(code, "AA") == 0)
				age = 14;
			else if (strcmp(code, "M 15+") == 0)
				age = 15;
			else if (strcmp(code, "MA 15+") == 0)
				age = 15;
			else if (strcmp(code, "R") == 0)
				age = 17;
			else if (strcmp(code, "TVMA") == 0)
				age = 17;
			else if (strcmp(code, "C") == 0)
				age = 18;
			else if (strcmp(code, "NC-17") == 0)
				age = 18;
			else
			{
				age = atoi(code);
				if (age == 0)
					printf("body=%s, code=%s\n", body, code);
			}

			save |= epg_episode_set_age_rating(ee, age, mod);
		}
	}

	return save;
}

static int sd_parse_air_date(
	void *mod,
	epg_episode_t *ee,
	htsmsg_t *episode)
{
	int save = 0;
	time_t date;
	const char *air;

	air = htsmsg_get_str(episode, "originalAirDate");
	if (air)
	{
		date = _sp_str2date(air);

		save |= epg_episode_set_first_aired(ee, date, mod);
	}

	return save;
}

static int sd_parse_movie(
	void *mod,
	epg_episode_t *ee,
	htsmsg_t *episode)
{
	int save = 0;
	htsmsg_t *movie, *quality, *m;
	htsmsg_field_t *f;
	const char *rating_s, *min_s, *max_s;
	double rating_d, min_d, max_d;

	movie = htsmsg_get_map(episode, "movie");
	if (movie)
	{
		quality = htsmsg_get_list(movie, "qualityRating");
		if (quality)
		{
			HTSMSG_FOREACH(f, quality)
			{
				m = htsmsg_get_map_by_field(f);

				rating_s = htsmsg_get_str(m, "rating");
				min_s = htsmsg_get_str(m, "minRating");
				max_s = htsmsg_get_str(m, "maxRating");

				if (rating_s && min_s && max_s)
				{
					rating_d = strtod(rating_s, NULL);
					min_d = strtod(min_s, NULL);
					max_d = strtod(max_s, NULL);

					save |= epg_episode_set_star_rating(ee, (100 * rating_d) / (max_d + min_d), mod);
				}

			}
		}
	}

	return save;
}

static int process_episode(
	void *mod,
	epg_episode_t *ee,
	epg_episode_num_t *epnum,
	const char *id,
	htsmsg_t *episode)
{
	int save = 0;
	const char *md5;

	if (sscanf(&id[10], "%hu", &epnum->e_num))
	{
		if (epnum->e_num)
		{
			save |= epg_episode_set_epnum(ee, epnum, mod);
		}
	}

	if (episode)
	{
		md5 = htsmsg_get_str(episode, "md5");

		save |= epg_episode_set_md5(ee, md5, mod);

		save |= sd_parse_titles(mod, ee, episode);

		save |= sd_parse_metadata(mod, ee, epnum, episode);

		save |= sd_parse_genre(mod, ee, episode);

		save |= sd_parse_content_rating(mod, ee, episode);

		save |= sd_parse_air_date(mod, ee, episode);

		save |= sd_parse_movie(mod, ee, episode);
	}

	return save;
}

static int sd_parse_multipart(
	void *mod,
	epg_episode_num_t *epnum,
	htsmsg_t *program)
{
	int save = 0;
	htsmsg_t *multipart;
	int tmp;

	multipart = htsmsg_get_map(program, "multipart");
	if (multipart)
	{
		htsmsg_get_s32(multipart, "partNumber", &tmp);
		epnum->p_num = tmp;
		htsmsg_get_s32(multipart, "totalParts", &tmp);
		epnum->p_cnt = tmp;
	}

	return save;
}

static int sd_parse_audio(
	void *mod,
	epg_broadcast_t *ebc,
	htsmsg_t *program)
{
	int save = 0;
	htsmsg_t *audio;
	htsmsg_field_t *f;
	const char *prop;

	audio = htsmsg_get_list(program, "audioProperties");
	if (audio)
	{
		HTSMSG_FOREACH(f, audio)
		{
			prop = htsmsg_field_get_str(f);

			if (strcmp(prop, "cc") == 0)
				save |= epg_broadcast_set_is_subtitled(ebc, 1, mod);
			else if (strcmp(prop, "subtitled") == 0)
				save |= epg_broadcast_set_is_subtitled(ebc, 1, mod);
			else if (strcmp(prop, "stereo") == 0)
				;
			else if (strcmp(prop, "dvs") == 0)
				 save |= epg_broadcast_set_is_audio_desc(ebc, 1, mod);
			else if (strcmp(prop, "DD 5.1") == 0)
				;
			else
				printf("audio prop=%s\n", prop);
		}
	}

	return save;
}

static int sd_parse_video(
	void *mod,
	epg_broadcast_t *ebc,
	htsmsg_t *program)
{
	int save = 0;
	htsmsg_t *video;
	htsmsg_field_t *f;
	const char *prop;

	video = htsmsg_get_list(program, "videoProperties");
	if (video)
	{
		HTSMSG_FOREACH(f, video)
		{
			prop = htsmsg_field_get_str(f);

			if (strcmp(prop, "hdtv") == 0)
				save |= epg_broadcast_set_is_hd(ebc, 1, mod);
			else if (strcmp(prop, "letterbox") == 0)
				;
			else
				printf("video prop=%s\n", prop);
		}
	}

	return save;
}

static htsmsg_field_t *htsmsg_add_uniq_str(
	htsmsg_t *msg,
	const char *name,
	const char *str)
{
	htsmsg_field_t *f;
	const char *str2;
	int len = strlen(str);

	HTSMSG_FOREACH(f, msg)
	{
		str2 = htsmsg_field_get_str(f);
		if (len == strlen(str2) && strcmp(str, str2) == 0)
			return f;
	}

	htsmsg_add_str(msg, name, str);

	return NULL;
}

static int process_program(
	void *mod,
	channel_t *ch,
	htsmsg_t *program,
	htsmsg_t *episode,
	epggrab_stats_t *stats)
{
	const char *id, *s;
	uint32_t duration;
	int start, stop, save = 0, save2 = 0, save3 = 0;
	epg_broadcast_t *ebc;
	int is_new = 0;
	char uri[128];
	char suri[128];
	epg_episode_t *ee = NULL;
	epg_serieslink_t *es = NULL;
	epg_episode_num_t epnum;

	memset(&epnum, 0, sizeof(epnum));

	id = htsmsg_get_str(program, "programID");
	s = htsmsg_get_str(program, "airDateTime");
	htsmsg_get_u32(program, "duration", &duration);

	start = _sp_str2time(s);
	stop = start + duration;

	if (stop <= start || stop <= dispatch_clock)
		return 0;

	if (!(ebc = epg_broadcast_find_by_time(ch, start, stop, 0, 1, &save)))
		return 0;
	stats->broadcasts.total ++;
	if (save) stats->broadcasts.created ++;

	htsmsg_get_bool(program, "new", &is_new);
	if (is_new)
		save |= epg_broadcast_set_is_new(ebc, 1, mod);

	save |= sd_parse_multipart(mod, &epnum, program);

	save |= sd_parse_audio(mod, ebc, program);

	save |= sd_parse_video(mod, ebc, program);

	if (episode)
		save |= sd_parse_description(mod, ebc, episode);

	snprintf(uri, sizeof(uri)-1, "ddprogid://%s/%s", ((epggrab_module_t *)mod)->id, id);
	if (strncmp(id, "SH", 2) == 0 || strncmp(id, "EP", 2) == 0)
	{
		snprintf(suri, sizeof(suri)-1, "ddprogid://%s/%.10s", ((epggrab_module_t *)mod)->id, id);

		es = epg_serieslink_find_by_uri(suri, 1, &save2);
		if (es)
		{
			stats->seasons.total ++;
			if (save2) stats->seasons.created ++;

			save |= epg_broadcast_set_serieslink(ebc, es, mod);
		}
	}

	ee = epg_episode_find_by_uri(uri, 1, &save3);
	if (ee)
	{
		stats->episodes.total ++;
		if (save3) stats->episodes.created ++;

		save |= epg_broadcast_set_episode(ebc, ee, mod);

		save |= sd_parse_content_rating(mod, ee, program);

		save |= process_episode(mod, ee, &epnum, id, episode);
	}

	if (save) stats->broadcasts.modified ++;
	if (save2) stats->seasons.modified ++;
	if (save3) stats->episodes.modified ++;

	return save | save2 | save3;
}

static void process_schedule_md5(
	void *mod,
	htsmsg_field_t *f,
	htsmsg_t *l,
	int *cnt)
{
	epggrab_module_sd_t *skel = (epggrab_module_sd_t *)mod;
	htsmsg_t *m, *m2, *m3;
	htsmsg_field_t *f2;
	const char *station_id, *md5;
	int save = 0;
	epggrab_channel_t *ch;

	station_id = f->hmf_name;
	m = htsmsg_get_list_by_field(f);
	ch = epggrab_channel_find(skel->mod.channels,
		station_id, 0, &save,
		(epggrab_module_t *)&skel->mod);

	HTSMSG_FOREACH(f2, m)
	{
		m2 = htsmsg_get_map_by_field(f2);
		md5 = htsmsg_get_str(m2, "md5");

		if (!skel->flush && ch->md5 && strcmp(ch->md5, md5) == 0)
			return;
	}

	m3 = htsmsg_create_map();
	htsmsg_add_str(m3, "stationID", station_id);
	htsmsg_add_u32(m3, "days", 13);
	htsmsg_add_msg(l, NULL, m3);
	(*cnt) ++;
}

static void process_schedule(
	void *mod,
	htsmsg_t *schedule,
	htsmsg_t *l,
	int *cnt)
{
	epggrab_module_sd_t *skel = (epggrab_module_sd_t *)mod;
	htsmsg_t *programs, *program;
	htsmsg_field_t *f;
	const char *program_id, *s, *md5;
	epg_episode_t *ee;
	char uri[128];
	int start, stop;
	uint32_t duration;
	int save = 0;
	int code = -1;

	if (!htsmsg_get_s32(schedule, "code", &code))
	{
		printf("Error: code=%d\n", code);
		htsmsg_print(schedule);
	}
	else
	{
		programs = htsmsg_get_list(schedule, "programs");
		HTSMSG_FOREACH(f, programs)
		{
			program = htsmsg_get_map_by_field(f);

			program_id = htsmsg_get_str(program, "programID");
			s = htsmsg_get_str(program, "airDateTime");
			htsmsg_get_u32(program, "duration", &duration);
			md5 = htsmsg_get_str(program, "md5");

			start = _sp_str2time(s);
			stop = start + duration;

			if (stop > start && stop > dispatch_clock)
			{
				snprintf(uri, sizeof(uri)-1, "ddprogid://%s/%s", ((epggrab_module_t *)mod)->id, program_id);
				ee = epg_episode_find_by_uri(uri, 0, &save);
				if (skel->flush || ee == NULL || epg_episode_md5_cmp(ee, md5))
				{
					if (htsmsg_add_uniq_str(l, NULL, program_id) == NULL)
						(*cnt) ++;
				}
			}
		}
	}
}

static char *_sd_grab(void *mod)
{
	epggrab_module_sd_t *skel = (epggrab_module_sd_t *)mod;
	CURL *curl;
	struct curl_slist *chunk = NULL;
	char token[33];
	char *ret;

	curl = curl_easy_init();
	chunk = curl_slist_append(chunk, "Content-Type: application/json;charset=UTF-8");
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
	curl_easy_setopt(curl, CURLOPT_ENCODING, "");
	curl_easy_setopt(curl, CURLOPT_USERAGENT, "benjamin.fennema@gmail.com (tvheadend)");

	if (get_token(curl, skel->username, skel->sha1_password, token) != 0)
		ret = NULL;
	else
		ret = token;

	curl_slist_free_all(chunk);
	curl_easy_cleanup(curl);

	return ret;
}


static int _sd_parse(
	void *mod,
	htsmsg_t *data,
	epggrab_stats_t *stats)
{
	epggrab_module_sd_t *skel = (epggrab_module_sd_t *)mod;
	htsmsg_t *schedules, *episodes, *episode, *m, *programs, *program, *metadata;
	htsmsg_field_t *f, *f2;
	const char *station_id, *program_id, *md5;
	epggrab_channel_t *ch;
	epggrab_channel_link_t *ecl;
	int save = 0, save2 = 0;
	int code = -1;

	schedules = htsmsg_get_map(data, "schedules");
	episodes = htsmsg_get_map(data, "episodes");

	if (schedules)
	{
		HTSMSG_FOREACH(f, schedules)
		{
			m = htsmsg_get_map_by_field(f);

			if (htsmsg_get_s32(m, "code", &code))
			{
				station_id = htsmsg_get_str(m, "stationID");
				programs = htsmsg_get_list(m, "programs");
				metadata = htsmsg_get_map(m, "metadata");
				md5 = htsmsg_get_str(metadata, "md5");

				ch = epggrab_channel_find(skel->mod.channels,
					station_id, 0, &save,
					(epggrab_module_t *)&skel->mod);
				stats->channels.total ++;
				save |= epggrab_channel_set_md5(ch, md5);
				if (save)
				{
					epggrab_channel_updated(ch);
					stats->channels.modified ++;
				}

				HTSMSG_FOREACH(f2, programs)
				{
					program = htsmsg_get_map_by_field(f2);
					program_id = htsmsg_get_str(program, "programID");
					if (episodes)
						episode = htsmsg_get_map(episodes, program_id);
					else
						episode = NULL;

					LIST_FOREACH(ecl, &ch->channels, ecl_epg_link)
					{
						save2 |= process_program(mod, ecl->ecl_channel, program, episode, stats);
					}
				}
			}
		}
	}

	if (skel->flush)
	{
		m = htsmsg_create_map();
		skel->flush = 0;

		htsmsg_add_str(m, "username", skel->username);
		htsmsg_add_str(m, "password", skel->sha1_password);
		htsmsg_add_bool(m, "flush", skel->flush);
		htsmsg_add_u32(m, "update", skel->update);

		hts_settings_save(m, "epggrab/%s/config", ((epggrab_module_t *)mod)->id);
	}

	return save | save2;
}

static htsmsg_t *_sd_trans(void *mod, char *data)
{
	epggrab_module_sd_t *skel = (epggrab_module_sd_t *)mod;
	CURL *curl;
	struct curl_slist *chunk = NULL;
	char token_header[40];
	const char *uri, *sid, *program_id;
	htsmsg_t *m, *v, *c, *m2, *v2, *c2, *c3, *m3, *s, *l, *n, *t, *msg, *ret;
	htsmsg_field_t *f, *f2;
	uint32_t major, minor, freq;
	epggrab_channel_t *ch;
	char name[64];
	int save = 0;
	int cnt = 0;
	int i;

	if (data == NULL)
		return NULL;

	curl = curl_easy_init();
	chunk = curl_slist_append(chunk, "Content-Type: application/json;charset=UTF-8");
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
	curl_easy_setopt(curl, CURLOPT_ENCODING, "");
	curl_easy_setopt(curl, CURLOPT_USERAGENT, "benjamin.fennema@gmail.com (tvheadend)");

	snprintf(token_header, sizeof(token_header), "Token: %s", data);
	chunk = curl_slist_append(chunk, token_header);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);

	if ((m = get_status(curl)) == NULL)
	{
		curl_slist_free_all(chunk);
		curl_easy_cleanup(curl);

		return NULL;
	}

	v = htsmsg_get_list(m, "lineups");
	if (!TAILQ_FIRST(&v->hm_fields))
	{
		htsmsg_destroy(m);

		add_lineup(curl, "/20140530/lineups/USA-OTA-94024");

		m = get_lineups(curl);

		v = htsmsg_get_list(m, "lineups");
	}

	l = htsmsg_create_list();

	HTSMSG_FOREACH(f, v)
	{
		c = htsmsg_get_map_by_field(f);
		uri = htsmsg_get_str(c, "uri");
		m2 = get_stations(curl, uri);
		if (m2)
		{
			m3 = htsmsg_create_map();

			if ((v2 = htsmsg_get_list(m2, "stations")) == NULL)
			{
				curl_slist_free_all(chunk);
				curl_easy_cleanup(curl);

				htsmsg_destroy(m3);
				htsmsg_destroy(m2);
				htsmsg_destroy(m);

				return NULL;
			}

			HTSMSG_FOREACH(f2, v2)
			{
				s = htsmsg_detach_submsg(f2);
				sid = htsmsg_get_str(s, "stationID");
				htsmsg_add_msg(m3, sid, s);
			}

			if ((v2 = htsmsg_get_list(m2, "map")) == NULL)
			{
				curl_slist_free_all(chunk);
				curl_easy_cleanup(curl);

				htsmsg_destroy(m3);
				htsmsg_destroy(m2);
				htsmsg_destroy(m);

				return NULL;
			}

			HTSMSG_FOREACH(f2, v2)
			{
				c2 = htsmsg_get_map_by_field(f2);
				sid = htsmsg_get_str(c2, "stationID");
				c3 = htsmsg_get_map(m3, sid);

				freq = major = minor = 0;
				htsmsg_get_u32(c2, "uhfVhf", &freq);
				htsmsg_get_u32(c2, "atscMajor", &major);
				htsmsg_get_u32(c2, "atscMinor", &minor);

				ch = epggrab_channel_find(skel->mod.channels,
					sid, 1, &save,
					(epggrab_module_t *)&skel->mod);
				sprintf(name, "%d-%d %s (%d)",
					major, minor, htsmsg_get_str(c3, "callsign"), freq);
				save |= epggrab_channel_set_name(ch, name);
				if (save)
					epggrab_channel_updated(ch);

				if (LIST_FIRST(&ch->channels))
				{
					n = htsmsg_create_map();
					htsmsg_add_str(n, "stationID", sid);
					htsmsg_add_u32(n, "days", 13);
					htsmsg_add_msg(l, NULL, n);
					cnt ++;
				}
			}
			htsmsg_destroy(m3);
			htsmsg_destroy(m2);
		}
	}

	htsmsg_destroy(m);

	ret = htsmsg_create_map();

	if (cnt > 0)
	{
		tvhlog(LOG_INFO, "sd", "Downloading %d schedule md5s\n", cnt);
		msg = get_schedules_md5(curl, l);

		cnt = 0;
		l = htsmsg_create_list();

		HTSMSG_FOREACH(f, msg)
		{
			process_schedule_md5(mod, f, l, &cnt);
		}
		htsmsg_destroy(msg);
	}

	if (cnt > 0)
	{
		tvhlog(LOG_INFO, "sd", "Downloading %d schedules\n", cnt);
		msg = get_schedules(curl, l);
		cnt = 0;

		l = htsmsg_create_list();

		pthread_mutex_lock(&global_lock);

		HTSMSG_FOREACH(f, msg)
		{
			m = htsmsg_get_map_by_field(f);

			process_schedule(mod, m, l, &cnt);
		}

		pthread_mutex_unlock(&global_lock);

		htsmsg_add_msg(ret, "schedules", msg);
	}

	if (cnt > 0)
	{
		tvhlog(LOG_INFO, "sd", "Downloading %d episodes\n", cnt);
		msg = htsmsg_create_map();
		t = htsmsg_create_list();
		i = 0;
		HTSMSG_FOREACH(f, l)
		{
			if (i >= 5000)
			{
				get_episodes(curl, msg, t);
				t = htsmsg_create_list();
				i = 0;
			}
			i++;
			program_id = htsmsg_field_get_str(f);
			htsmsg_add_str(t, 0, program_id);
		}
		get_episodes(curl, msg, t);
		htsmsg_add_msg(ret, "episodes", msg);
	}
	else
		htsmsg_destroy(l);

	curl_slist_free_all(chunk);
	curl_easy_cleanup(curl);

	return ret;
}

void sd_init(void)
{
	epggrab_module_sd_t *skel = calloc(1, sizeof(epggrab_module_sd_t));
	htsmsg_t *m;
	const char *username, *password;

	curl_global_init(CURL_GLOBAL_DEFAULT);

	m = hts_settings_load("epggrab/sd/config");

	if (m)
	{
		username = htsmsg_get_str(m, "username");
		password = htsmsg_get_str(m, "password");
		skel->flush = htsmsg_get_bool_or_default(m, "flush", 0);
		skel->update = htsmsg_get_u32_or_default(m, "update", dispatch_clock);

		if (username)
			strcpy(skel->username, username);
		if (password)
			strcpy(skel->sha1_password, password);

		htsmsg_destroy(m);
	}

	epggrab_module_int_create(&skel->mod, "sd", "Schedules Direct", 4, "sd",
			_sd_grab, _sd_parse, _sd_trans, &_sd_channels);
}

void sd_load(void)
{
	epggrab_module_channels_load(epggrab_module_find_by_id("sd"));
}
