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
	char token[33];
	char username[64];
	char sha1_password[SHA_DIGEST_LENGTH*2+1];
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

	curl_easy_setopt(curl, CURLOPT_URL, "https://json.schedulesdirect.org/20131021/token");
	curl_easy_setopt(curl, CURLOPT_POST, 1L);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, out);
#if 0
	curl_easy_setopt(curl, CURLOPT_VERBOSE, 1);
#endif
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

	curl_easy_perform(curl);

        free(out);

	m = htsmsg_json_deserialize(buf.ptr);
	if (!htsmsg_get_s32(m, "code", &code) && code == 0)
	{
		if ((ptr = htsmsg_get_str(m, "token")))
			strcpy(token, ptr);
	}
	htsmsg_destroy(m);
	free(buf.ptr);
	return code;
}

static htsmsg_t *get_status(CURL *curl)
{
	struct buffer buf = { 0, 0, NULL };
	htsmsg_t *m;

	curl_easy_setopt(curl, CURLOPT_URL, "https://json.schedulesdirect.org/20131021/status");
	curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

	curl_easy_perform(curl);

/*	printf("ptr: %s\n", buf.ptr); */

	m = htsmsg_json_deserialize(buf.ptr);
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
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

	curl_easy_perform(curl);

	m = htsmsg_json_deserialize(buf.ptr);
	free(buf.ptr);

	return m;
}

static htsmsg_t *get_schedule(CURL *curl, const char *channel)
{
	char *out;
	struct buffer buf = { 0, 0, NULL };
	htsmsg_t *m, *l;

	l = htsmsg_create_list();
	htsmsg_add_str(l, NULL, channel);

	m = htsmsg_create_map();
	htsmsg_add_msg(m, "request", l);
	out = htsmsg_json_serialize_to_str(m, 0);
	htsmsg_destroy(m);

	curl_easy_setopt(curl, CURLOPT_URL, "https://json.schedulesdirect.org/20131021/schedules");
	curl_easy_setopt(curl, CURLOPT_POST, 1L);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, out);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

	curl_easy_perform(curl);

	free(out);

/*	printf("ptr: %s\n", buf.ptr);  */

	m = htsmsg_json_deserialize(buf.ptr);
	free(buf.ptr);

	return m;
}

static htsmsg_t *get_episode(CURL *curl, const char *program)
{
	char *out;
	struct buffer buf = { 0, 0, NULL };
	htsmsg_t *m, *l;

	l = htsmsg_create_list();
	htsmsg_add_str(l, NULL, program);

	m = htsmsg_create_map();
	htsmsg_add_msg(m, "request", l);
	out = htsmsg_json_serialize_to_str(m, 0);
	htsmsg_destroy(m);

	curl_easy_setopt(curl, CURLOPT_URL, "https://json.schedulesdirect.org/20131021/programs");
	curl_easy_setopt(curl, CURLOPT_POST, 1L);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, out);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

	curl_easy_perform(curl);

	free(out);

/*	printf("ptr: %s\n", buf.ptr);  */

	m = htsmsg_json_deserialize(buf.ptr);
	free(buf.ptr);

	return m;
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

static void process_episode(
	void *mod,
	epg_episode_t *ee,
	htsmsg_t *episode)
{
	const char *md5, *title, *subtitle;
	int save = 0;
	htsmsg_t *titles;
	htsmsg_t *metadata;
	htsmsg_t *m;
	htsmsg_field_t *f;
	htsmsg_t *tribune;
	htsmsg_t *descriptions, *desc1;
	htsmsg_t *genre;
	const char *g;
	const char *desc, *lang;
	epg_episode_num_t epnum;
	int tmp;
	epg_genre_list_t genres;
	memset(&genres, 0x00, sizeof(epg_genre_list_t));
	const char *air;
	time_t date;

	memset(&epnum, 0, sizeof(epnum));

	md5 = htsmsg_get_str(episode, "md5");

	save |= epg_episode_set_md5(ee, md5, mod);

	titles = htsmsg_get_map(episode, "titles");

	title = htsmsg_get_str(titles, "title120");

	save |= epg_episode_set_title(ee, title, "en", mod);

	subtitle = htsmsg_get_str(episode, "episodeTitle150");

	save |= epg_episode_set_subtitle(ee, subtitle, "en", mod);

	metadata = htsmsg_get_list(episode, "metadata");
	if (metadata)
	{
	        HTSMSG_FOREACH(f, metadata)
	        {
	                m = htsmsg_get_map_by_field(f);
			tribune = htsmsg_get_map(m, "Tribune");

			htsmsg_get_s32(tribune, "season", &tmp);
			epnum.s_num = tmp;
			htsmsg_get_s32(tribune, "episode", &tmp);
			epnum.e_num = tmp;

			save |= epg_episode_set_epnum(ee, &epnum, mod);
	        }
	}

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

				save |= epg_episode_set_description(ee, desc, lang, mod);
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

				save |= epg_episode_set_summary(ee, desc, lang, mod);
			}
		}
	}

	genre = htsmsg_get_list(episode, "genres");
	if (genre)
	{
		HTSMSG_FOREACH(f, genre)
		{
			g = htsmsg_field_get_str(f);
			epg_genre_list_add_by_str(&genres, g);
		}

		save |= epg_episode_set_genre(ee, &genres, mod);
	}


//	epg_episode_set_age_rating

	air = htsmsg_get_str(episode, "originalAirDate");
	if (air)
	{
		date = _sp_str2date(air);

		save |= epg_episode_set_first_aired(ee, date, mod);
	}

}

static void process_program(
	void *mod,
	CURL *curl,
	channel_t *ch,
	const char *station,
	htsmsg_t *program)
{
	const char *id, *md5, *s;
	uint32_t duration;
	int start, stop, save = 0, save2 = 0, save3 = 0;
	epg_broadcast_t *ebc;
	int is_new = 0;
	char uri[128];
	char suri[128];
	epg_episode_t *ee = NULL;
	epg_serieslink_t *es = NULL;
	htsmsg_t *episode;

	id = htsmsg_get_str(program, "programID");
	md5 = htsmsg_get_str(program, "md5");
	s = htsmsg_get_str(program, "airDateTime");
	htsmsg_get_u32(program, "duration", &duration);

	start = _sp_str2time(s);
	stop = start + duration;

	if (stop <= start || stop <= dispatch_clock)
		return;

	pthread_mutex_lock(&global_lock);
	if (!(ebc = epg_broadcast_find_by_time(ch, start, stop, 0, 1, &save)))
	{
		pthread_mutex_unlock(&global_lock);
		return;
	}

	htsmsg_get_bool(program, "new", &is_new);
	if (is_new)
		save |= epg_broadcast_set_is_new(ebc, 1, mod);

	snprintf(uri, sizeof(uri)-1, "ddprogid://%s/%s", ((epggrab_module_t *)mod)->id, id);
	snprintf(suri, sizeof(suri)-1, "ddprogid://%s/%.10s", ((epggrab_module_t *)mod)->id, id);

	es = epg_serieslink_find_by_uri(suri, 1, &save2);
	if (es)
		save |= epg_broadcast_set_serieslink(ebc, es, mod);

	ee = epg_episode_find_by_uri(uri, 1, &save3);
	if (ee)
		save |= epg_broadcast_set_episode(ebc, ee, mod);
	pthread_mutex_unlock(&global_lock);

	if (epg_episode_md5_cmp(ee, md5))
	{
		episode = get_episode(curl, id);
		process_episode(mod, ee, episode);
	}
}

static void process_schedule(
	void *mod,
	CURL *curl,
	channel_t *ch,
	htsmsg_t *schedule,
	const htsmsg_t *station)
{
	htsmsg_t *programs, *program;
	htsmsg_field_t *f;
	const char *id;

	id = htsmsg_get_str(schedule, "stationID");
	programs = htsmsg_get_list(schedule, "programs");
	HTSMSG_FOREACH(f, programs)
	{
		program = htsmsg_get_map_by_field(f);
		process_program(mod, curl, ch, id, program);

	}

	htsmsg_destroy(schedule);
}

static char *_sd_grab(void *mod)
{
	epggrab_module_sd_t *skel = (epggrab_module_sd_t *)mod;
	CURL *curl;
	struct curl_slist *chunk = NULL;
	char token_header[40];
	const char *uri, *sid;
	htsmsg_t *m, *v, *c, *m2, *v2, *c2, *c3, *m3, *s, *schedule;
	htsmsg_field_t *f, *f2;
	uint32_t major, minor, freq;
	epggrab_channel_t *ch;
	epggrab_channel_link_t *ecl;
	char name[64];
	int save = 0;

	m = htsmsg_create_map();

	curl = curl_easy_init();
	chunk = curl_slist_append(chunk, "Content-Type: application/json;charset=UTF-8");
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
	curl_easy_setopt(curl, CURLOPT_ENCODING, "");


	if (strlen(skel->token) == 0)
	{
		get_token(curl, skel->username, skel->sha1_password, skel->token);
	}
	snprintf(token_header, sizeof(token_header), "Token: %s", skel->token);
	chunk = curl_slist_append(chunk, token_header);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);

	m = get_status(curl);
	v = htsmsg_get_list(m, "lineups");

	HTSMSG_FOREACH(f, v)
	{
		c = htsmsg_get_map_by_field(f);
		uri = htsmsg_get_str(c, "uri");
		m2 = get_stations(curl, uri);
		m3 = htsmsg_create_map();

		v2 = htsmsg_get_list(m2, "stations");
		HTSMSG_FOREACH(f2, v2)
		{
			s = htsmsg_detach_submsg(f2);
			sid = htsmsg_get_str(s, "stationID");
			htsmsg_add_msg(m3, sid, s);
		}

		v2 = htsmsg_get_list(m2, "map");
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
			sprintf(name, "%d-%d %s (%d)\n",
				major, minor, htsmsg_get_str(c3, "callsign"), freq);
			save |= epggrab_channel_set_name(ch, name);
			if (save)
				epggrab_channel_updated(ch);

			if (LIST_FIRST(&ch->channels))
			{
				schedule = get_schedule(curl, sid);

				LIST_FOREACH(ecl, &ch->channels, ecl_epg_link)
				{
					process_schedule(mod, curl, ecl->ecl_channel, schedule, c3);
				}

			}
		}
		htsmsg_destroy(m2);
		htsmsg_destroy(m3);
	}

	htsmsg_destroy(m);

	return NULL;
}

static int _sd_parse(
	void *mod,
	htsmsg_t *data,
	epggrab_stats_t *stats)
{
	return 0;
}

static htsmsg_t *_sd_trans(void *mod, char *data)
{
	return NULL;
}

void sd_init(void)
{
	epggrab_module_sd_t *skel = calloc(1, sizeof(epggrab_module_sd_t));
	htsmsg_t *m;

	m = hts_settings_load("epggrab/sd/config");

	strcpy(skel->username, htsmsg_get_str(m, "username"));
	strcpy(skel->sha1_password, htsmsg_get_str(m, "password"));

	htsmsg_destroy(m);

	epggrab_module_int_create(&skel->mod, "sd", "Schedules Direct", 3, "sd",
			_sd_grab, _sd_parse, _sd_trans, &_sd_channels);
}

void sd_load(void)
{
	epggrab_module_channels_load(epggrab_module_find_by_id("sd"));
}
