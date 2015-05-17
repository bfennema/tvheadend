/*
 *  Electronic Program Guide - schedules direct grabber
 *  Copyright (C) 2014-2015 Benjamin Fennema
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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

#define USERAGENT    "benjamin.fennema@gmail.com (tvheadend)"
#define UPDATE_INTERVAL    (60*60*6)

#define container_of(ptr, type, member) ({ \
                const typeof( ((type *)0)->member ) *__mptr = (ptr); \
                (type *)( (char *)__mptr - offsetof(type,member) );})

static epggrab_channel_tree_t    _sd_channels;

typedef struct idnode_sd
{
    idnode_t sd_id;
    const char *username;
    const char *sha1_password;
    time_t update;
    const char *status;
    time_t expiration;
    const char *country;
    const char *zipcode;
    int lineup[4];
} idnode_sd_t;

typedef struct epggrab_module_sd
{
    epggrab_module_int_t mod;
    idnode_sd_t node;
    int flush;
    int server_lineup[4];
    char *custom_lineup;
    htsmsg_t *lineups;
} epggrab_module_sd_t;

htsmsg_t *sd_get_token(epggrab_module_sd_t *skel, CURL **curl, struct curl_slist **chunk);
static htsmsg_t *get_status(CURL *curl);
static time_t _sp_str2time(const char *in);
static htsmsg_t *get_headends(CURL *curl, const char *country, const char *zipcode);
static htsmsg_t *delete_lineup(CURL *curl, const char *uri);
static htsmsg_t *add_lineup(CURL *curl, const char *uri);
static void settings_save(epggrab_module_sd_t *skel);

static void
sd_device_class_save ( idnode_t *in )
{
    epggrab_module_sd_t *skel = container_of(in, epggrab_module_sd_t, node.sd_id);
    htsmsg_t *m, *m2, *m3, *m4, *m5, *account, *lineups;
    CURL *curl;
    struct curl_slist *chunk;
    const char *token = NULL;
    char token_header[40];
    int code;
    const char *expiration, *name, *location, *uri, *id;
    htsmsg_field_t *f, *f2;
    int lineup_cnt = 0;
    int i, j, index;

    if ((m = sd_get_token(skel, &curl, &chunk)))
    {
        if (!htsmsg_get_s32(m, "code", &code) && code == 0)
            token = htsmsg_get_str(m, "token");

        skel->node.status = strdup(htsmsg_get_str(m, "message") ?: "");

        if (token)
        {
            snprintf(token_header, sizeof(token_header), "Token: %s", token);
            chunk = curl_slist_append(chunk, token_header);
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);

            m2 = get_status(curl);
            account = htsmsg_get_map(m2, "account");
            if ((expiration = htsmsg_get_str(account, "expires")) != NULL)
                skel->node.expiration = _sp_str2time(expiration);

            settings_save(skel);

            if ((lineups = htsmsg_get_list(m2, "lineups")) != NULL)
            {
                if (skel->lineups)
                {
                    HTSMSG_FOREACH(f, skel->lineups)
                        lineup_cnt ++;
                }
                else
                {
                    skel->lineups = htsmsg_create_map();
                    lineup_cnt = 0;
                    HTSMSG_FOREACH(f, lineups)
                    {
                        skel->node.lineup[lineup_cnt] = lineup_cnt + 1;
                        skel->server_lineup[lineup_cnt] = lineup_cnt + 1;
                        m3 = htsmsg_get_map_by_field(f);
                        m4 = htsmsg_create_map();
                        htsmsg_add_u32(m4, "index", ++lineup_cnt);
                        htsmsg_add_str(m4, "id", htsmsg_get_str(m3, "lineup"));
                        htsmsg_add_str(m4, "modified", htsmsg_get_str(m3, "modified"));
                        htsmsg_add_msg(skel->lineups, htsmsg_get_str(m3, "uri"), m4);
                    }

                }
            }
            htsmsg_destroy(m2);

            if (skel->custom_lineup)
            {
                printf("Add Custom:\n");
                m3 = add_lineup(curl, skel->custom_lineup);
                if (m3)
                {
                    htsmsg_print(m3);
                    m4 = htsmsg_create_map();
                    htsmsg_add_u32(m4, "index", ++lineup_cnt);
                    htsmsg_add_str(m4, "id", skel->custom_lineup+18);
                    htsmsg_add_msg(skel->lineups, skel->custom_lineup, m4);
                    htsmsg_destroy(m3);

                    for (j=0; j<4; j++)
                    {
                        if (skel->server_lineup[j] == 0)
                        {
                            skel->server_lineup[j] = lineup_cnt;
                            break;
                        }
                    }

                    for (j=0; j<4; j++)
                    {
                        if (skel->node.lineup[j] == 0)
                        {
                            skel->node.lineup[j] = lineup_cnt;
                            break;
                        }
                    }
                }
                free(skel->custom_lineup);
                skel->custom_lineup = NULL;
            }

            if (skel->node.country && skel->node.zipcode)
            {
                m2 = get_headends(curl, skel->node.country, skel->node.zipcode);
                if (m2)
                {
                    HTSMSG_FOREACH(f, m2)
                    {
                        m3 = htsmsg_get_map_by_field(f);
                        location = htsmsg_get_str(m3, "location");
                        lineups = htsmsg_get_list(m3, "lineups");

                        HTSMSG_FOREACH(f2, lineups)
                        {
                            m4 = htsmsg_get_map_by_field(f2);
                            name = htsmsg_get_str(m4, "name");
                            uri = htsmsg_get_str(m4, "uri");
                            id = uri + strlen("/20141201/lineups/");
                            if (!skel->lineups)
                                skel->lineups = htsmsg_create_map();
                            if ((m5 = htsmsg_get_map(skel->lineups, uri)) == NULL)
                            {
                                m5 = htsmsg_create_map();
                                htsmsg_add_s32(m5, "index", ++lineup_cnt);
                                htsmsg_add_str(m5, "id", id);
                                htsmsg_add_str(m5, "name", name);
                                htsmsg_add_str(m5, "location", location);
                                htsmsg_add_msg(skel->lineups, uri, m5);
                            }
                            else
                            {
                                htsmsg_add_str(m5, "name", name);
                                htsmsg_add_str(m5, "location", location);
                            }
                        }
                    }
                    htsmsg_destroy(m2);
                }
            }

            if (skel->lineups)
            {
                for (i=0; i<4; i++)
                {
                    for (j=0; j<4; j++)
                    {
                        if (skel->server_lineup[i] == skel->node.lineup[j])
                            break;
                    }
                    if (j == 4)
                    {
                        HTSMSG_FOREACH(f2, skel->lineups)
                        {
                            m2 = htsmsg_get_map_by_field(f2);
                            htsmsg_get_s32(m2, "index", &index);
                            if (index == skel->server_lineup[i])
                            {
                                m3 = delete_lineup(curl, f2->hmf_name);
                                printf("Delete:\n");
                                htsmsg_print(m3);
                                htsmsg_destroy(m3);
                                skel->server_lineup[i] = 0;
                                break;
                            }
                        }
                    }
                }
                for (i=0; i<4; i++)
                {
                    for (j=0; j<4; j++)
                    {
                        if (skel->node.lineup[i] == skel->server_lineup[j])
                            break;
                    }
                    if (j == 4)
                    {
                        HTSMSG_FOREACH(f2, skel->lineups)
                        {
                            m2 = htsmsg_get_map_by_field(f2);
                            htsmsg_get_s32(m2, "index", &index);
                            if (index == skel->node.lineup[i])
                            {
                                m3 = add_lineup(curl, f2->hmf_name);
                                printf("Add:\n");
                                htsmsg_print(m3);
                                htsmsg_destroy(m3);
                                for (j=0; j<4; j++)
                                {
                                    if (skel->server_lineup[j] == 0)
                                        skel->server_lineup[j] = index;
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
        htsmsg_destroy(m);
    }

    curl_slist_free_all(chunk);
    curl_easy_cleanup(curl);

}

static const char *
sd_device_class_get_title ( idnode_t *self )
{
    return "Schedules Direct";
}

static int
sd_device_class_custom_lineup_set( void *obj, const void *p )
{
    epggrab_module_sd_t *skel = container_of(obj, epggrab_module_sd_t, node.sd_id);
    const char lineup_base[] = "/20141201/lineups/";

    if (p && strlen(p))
    {
        skel->custom_lineup = calloc(1, strlen(lineup_base) + strlen(p) + 1);
        strcpy(skel->custom_lineup, lineup_base);
        strcat(skel->custom_lineup, p);
    }

    return 1;
}

static const void *
sd_device_class_custom_lineup_get( void *obj )
{
    static const char *str = "";
    return &str;
}

static int
sd_device_class_password_set( void *obj, const void *p )
{
    epggrab_module_sd_t *skel = container_of(obj, epggrab_module_sd_t, node.sd_id);
    unsigned char hash[SHA_DIGEST_LENGTH];
    char sha1_password[SHA_DIGEST_LENGTH*2+1];
    int i;

    if (skel->node.sha1_password && strlen(skel->node.sha1_password) == SHA_DIGEST_LENGTH*2 && strncmp(skel->node.sha1_password, p, SHA_DIGEST_LENGTH*2) == 0)
        return 1;

    SHA1(p, strlen(p), hash);

    for (i=0; i<SHA_DIGEST_LENGTH; i++)
    {
        sha1_password[i*2] = "0123456789abcdef"[hash[i] >> 4];
        sha1_password[i*2+1] = "0123456789abcdef"[hash[i] & 0x0F];
    }
    sha1_password[SHA_DIGEST_LENGTH*2] = '\0';
    skel->node.sha1_password = strdup(sha1_password);
    return 1;
}

static htsmsg_t *
sd_device_class_headend_list(void * obj)
{
    epggrab_module_sd_t *skel = NULL;
    htsmsg_t *l, *m, *m2;
    htsmsg_field_t *f;
    const char *name, *location, *id;
    char buf[80];
    int index = -1;

    if (obj)
        skel = container_of(obj, epggrab_module_sd_t, node.sd_id);

    l = htsmsg_create_list();

    m2 = htsmsg_create_map();

    htsmsg_add_s32(m2, "key", 0);
    htsmsg_add_str(m2, "val", "Unused");
    htsmsg_add_msg(l, NULL, m2);

    if (skel && skel->lineups)
    {
        HTSMSG_FOREACH(f, skel->lineups)
        {
            m = htsmsg_get_map_by_field(f);
            m2 = htsmsg_create_map();

            htsmsg_get_s32(m, "index", &index);
            htsmsg_add_s32(m2, "key", index);
            name = htsmsg_get_str(m, "name");
            location = htsmsg_get_str(m, "location");
            id = htsmsg_get_str(m, "id");
            if (name && location)
            {
                sprintf(buf, "%s (%s)", name, location);
                htsmsg_add_str(m2, "val", buf);
            }
            else if (name)
                htsmsg_add_str(m2, "val", name);
            else
                htsmsg_add_str(m2, "val", id);

            htsmsg_add_msg(l, NULL, m2);
        }
    }

    return l;
}

const idclass_t epggrab_sd_device_class = {
    .ic_class      = "epggrab_sd_client",
    .ic_caption    = "Schedules Direct",
    .ic_save       = sd_device_class_save,
    .ic_get_title  = sd_device_class_get_title,
    .ic_groups     = (const property_group_t[])
    {
        {
            .name      = "Account Information",
            .number    = 1,
        },
        {
            .name      = "Configure Lineup",
            .number    = 2,
        },
        {
            .name      = "Advanced",
            .number    = 3,
        },
        {}
    },
    .ic_properties = (const property_t[])
    {
        {
            .type      = PT_STR,
            .id        = "username",
            .name      = "Username",
            .off       = offsetof(idnode_sd_t, username),
            .group     = 1,
        },
        {
            .type      = PT_STR,
            .id        = "password",
            .name      = "Password",
            .opts      = PO_PASSWORD,
            .set       = sd_device_class_password_set,
            .off       = offsetof(idnode_sd_t, sha1_password),
            .group     = 1,
        },
        {
            .type      = PT_TIME,
            .id        = "update",
            .name      = "Last Update",
            .opts      = PO_RDONLY,
            .off       = offsetof(idnode_sd_t, update),
            .group     = 1,
        },
        {
            .type      = PT_STR,
            .id        = "status",
            .name      = "Status",
            .opts      = PO_RDONLY,
            .off       = offsetof(idnode_sd_t, status),
            .group     = 1,
        },
        {
            .type      = PT_TIME,
            .id        = "expiration",
            .name      = "Account Expiration",
            .opts      = PO_RDONLY,
            .off       = offsetof(idnode_sd_t, expiration),
            .group     = 1,
        },
        {
            .type      = PT_STR,
            .id        = "country",
            .name      = "Country",
            .opts      = PO_NOSAVE,
            .off       = offsetof(idnode_sd_t, country),
            .group     = 2,
        },
        {
            .type      = PT_STR,
            .id        = "zipcode",
            .name      = "Zipode",
            .opts      = PO_NOSAVE,
            .off       = offsetof(idnode_sd_t, zipcode),
            .group     = 2,
        },
        {
            .type      = PT_INT,
            .id        = "lineup1",
            .name      = "Lineup 1",
            .off       = offsetof(idnode_sd_t, lineup[0]),
            .list      = sd_device_class_headend_list,
            .group     = 2,
        },
        {
            .type      = PT_INT,
            .id        = "lineup2",
            .name      = "Lineup 2",
            .off       = offsetof(idnode_sd_t, lineup[1]),
            .list      = sd_device_class_headend_list,
            .group     = 2,
        },
        {
            .type      = PT_INT,
            .id        = "lineup3",
            .name      = "Lineup 3",
            .off       = offsetof(idnode_sd_t, lineup[2]),
            .list      = sd_device_class_headend_list,
            .group     = 2,
        },
        {
            .type      = PT_INT,
            .id        = "lineup4",
            .name      = "Lineup 4",
            .off       = offsetof(idnode_sd_t, lineup[3]),
            .list      = sd_device_class_headend_list,
            .group     = 2,
        },
        {
            .type      = PT_STR,
            .id        = "custom",
            .name      = "Custom Lineup",
            .set       = sd_device_class_custom_lineup_set,
            .get       = sd_device_class_custom_lineup_get,
            .group     = 3,
        },
        {}
    },
};

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

static htsmsg_t *get_token(CURL *curl, const char *username, const char *sha1_hex)
{
    char *out;
    struct buffer buf = { 0, 0, NULL };
    htsmsg_t *m;
    CURLcode ret;

    m = htsmsg_create_map();
    htsmsg_add_str(m, "username", username);
    htsmsg_add_str(m, "password", sha1_hex);
    out = htsmsg_json_serialize_to_str(m, 0);
    htsmsg_destroy(m);

    curl_easy_setopt(curl, CURLOPT_URL, "https://json.schedulesdirect.org/20141201/token");
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, out);
#if 0
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1);
#endif
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

    ret = curl_easy_perform(curl);

    free(out);

    if (ret == CURLE_OK)
    {
        buf.ptr[buf.cur_size] = '\0';
        m = htsmsg_json_deserialize(buf.ptr);
        free(buf.ptr);
        return m;
    }
    else
    {
        if (buf.ptr)
            free(buf.ptr);
        return NULL;
    }
}

static htsmsg_t *add_lineup(CURL *curl, const char *uri)
{
    char url[160];
    int code = -1;
    struct buffer buf = { 0, 0, NULL };
    htsmsg_t *m = NULL;

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

static htsmsg_t *delete_lineup(CURL *curl, const char *uri)
{
    char url[160];
    int code = -1;
    struct buffer buf = { 0, 0, NULL };
    htsmsg_t *m = NULL;

    snprintf(url, sizeof(url), "https://json.schedulesdirect.org%s", uri);

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
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

    curl_easy_setopt(curl, CURLOPT_URL, "https://json.schedulesdirect.org/20141201/status");
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

static htsmsg_t *get_headends(CURL *curl, const char *country, const char *zipcode)
{
    char url[160];
    struct buffer buf = { 0, 0, NULL };
    htsmsg_t *m = NULL;

    snprintf(url, sizeof(url), "https://json.schedulesdirect.org/20141201/headends?country=%s&postalcode=%s", country, zipcode);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

    curl_easy_perform(curl);

    if (buf.cur_size > 0)
    {
        buf.ptr[buf.cur_size] = '\0';
        m = htsmsg_json_deserialize(buf.ptr);
    }
    free(buf.ptr);
    return m;
}

static htsmsg_t __attribute__((unused)) *get_lineups(CURL *curl)
{
    int code = -1;
    struct buffer buf = { 0, 0, NULL };
    htsmsg_t *m = NULL;

    curl_easy_setopt(curl, CURLOPT_URL, "https://json.schedulesdirect.org/20141201/lineups");
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

    curl_easy_setopt(curl, CURLOPT_URL, "https://json.schedulesdirect.org/20141201/schedules/md5");
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
    struct buffer buf = { 0, 0, NULL };
    htsmsg_t *m;

    out = htsmsg_json_serialize_to_str(l, 0);
    htsmsg_destroy(l);

    curl_easy_setopt(curl, CURLOPT_URL, "https://json.schedulesdirect.org/20141201/schedules");
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, out);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

    curl_easy_perform(curl);

    free(out);

    buf.ptr[buf.cur_size] = '\0';
    m = htsmsg_json_deserialize(buf.ptr);
    if (m == NULL)
    {
        printf("get_schedules() error: len=%d, ptr=%s\n", buf.cur_size, buf.ptr);
    }
    free(buf.ptr);

    return m;
}

static htsmsg_t *get_episodes(CURL *curl, htsmsg_t *l)
{
    char *out;
    struct buffer buf = { 0, 0, NULL };
    htsmsg_t *m;

    out = htsmsg_json_serialize_to_str(l, 0);
    htsmsg_destroy(l);

    curl_easy_setopt(curl, CURLOPT_URL, "https://json.schedulesdirect.org/20141201/programs");
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, out);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

    curl_easy_perform(curl);

    free(out);

    buf.ptr[buf.cur_size] = '\0';
    m = htsmsg_json_deserialize(buf.ptr);
    if (m == NULL)
    {
        printf("get_episodes) error: len=%d, ptr=%s\n", buf.cur_size, buf.ptr);
    }
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

static int sd_parse_titles(
    epggrab_module_sd_t *skel,
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
                save |= epg_episode_set_title(ee, title, "en", (epggrab_module_t *)&skel->mod);
            }
        }
    }

    subtitle = htsmsg_get_str(episode, "episodeTitle150");

    if (subtitle)
    {
        save |= epg_episode_set_subtitle(ee, subtitle, "en", (epggrab_module_t *)&skel->mod);
    }

    return save;
}

static int sd_parse_metadata(
    epggrab_module_sd_t *skel,
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
                if (strcmp(g->hmf_name, "Gracenote"))
                    printf("Provider: %s\n", g->hmf_name);

                if (!htsmsg_get_s32(provider, "episode", &tmp))
                {
                    epnum->e_num = tmp;

                    if (!htsmsg_get_s32(provider, "season", &tmp))
                        epnum->s_num = tmp;
                    save |= epg_episode_set_epnum(ee, epnum, (epggrab_module_t *)&skel->mod);
                }
            }
        }
    }

    return save;
}

static int sd_parse_description(
    epggrab_module_sd_t *skel,
    epg_episode_t *ee,
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

                save |= epg_episode_set_description(ee, desc, lang, (epggrab_module_t *)&skel->mod);
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

                save |= epg_episode_set_summary(ee, desc, lang, (epggrab_module_t *)&skel->mod);
            }
        }
    }

    return save;
}

static int sd_parse_genre(
    epggrab_module_sd_t *skel,
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
            save |= epg_episode_set_genre(ee, genres, (epggrab_module_t *)&skel->mod);
            epg_genre_list_destroy(genres);
        }
    }

    return save;
}

static int sd_parse_content_rating(
    epggrab_module_sd_t *skel,
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
            else if (strcmp(code, "S") == 0)
                age = 0;
            else if (strcmp(code, "0") == 0)
                age = 0;
            else if (strcmp(code, "TP") == 0)
                age = 0;
            else if (strcmp(body, "Canadian Parental Rating") == 0 && strcmp(code, "C") == 0)
                age = 1;
            else if (strcmp(code, "TVY7") == 0)
                age = 7;
            else if (strcmp(code, "K7") == 0)
                age = 7;
            else if (strcmp(code, "PG") == 0)
                age = 10;
            else if (strcmp(code, "GP") == 0)
                age = 10;
            else if (strcmp(code, "TVPG") == 0)
                age = 10;
            else if (strcmp(code, "B") == 0)
                age = 12;
            else if (strcmp(code, "K12") == 0)
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
            else if (strcmp(code, "K16") == 0)
                age = 16;
            else if (strcmp(code, "R") == 0)
                age = 17;
            else if (strcmp(code, "TVMA") == 0)
                age = 17;
            else if (strcmp(code, "X") == 0)
                age = 17;
            else if (strcmp(code, "NC-17") == 0)
                age = 17;
            else if (strcmp(code, "C") == 0)
                age = 18;
            else if (strcmp(code, "K18") == 0)
                age = 18;
            else if (strcmp(code, "R 18+") == 0)
                age = 18;
            else
            {
                age = atoi(code);
                if (age == 0)
                    printf("body=%s, code=%s\n", body, code);
            }

            save |= epg_episode_set_age_rating(ee, age, (epggrab_module_t *)&skel->mod);
        }
    }

    return save;
}

static int sd_parse_air_date(
    epggrab_module_sd_t *skel,
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

        save |= epg_episode_set_first_aired(ee, date, (epggrab_module_t *)&skel->mod);
    }

    return save;
}

static int sd_parse_movie(
    epggrab_module_sd_t *skel,
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

                    save |= epg_episode_set_star_rating(ee, (100 * rating_d) / (max_d + min_d), (epggrab_module_t *)&skel->mod);
                }

            }
        }
    }

    return save;
}

static int process_episode(
    epggrab_module_sd_t *skel,
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
            save |= epg_episode_set_epnum(ee, epnum, (epggrab_module_t *)&skel->mod);
        }
    }

    md5 = htsmsg_get_str(episode, "md5");

    save |= epg_episode_set_md5(ee, md5, (epggrab_module_t *)&skel->mod);

    save |= sd_parse_titles(skel, ee, episode);

    save |= sd_parse_metadata(skel, ee, epnum, episode);

    save |= sd_parse_description(skel, ee, episode);

    save |= sd_parse_genre(skel, ee, episode);

    save |= sd_parse_content_rating(skel, ee, episode);

    save |= sd_parse_air_date(skel, ee, episode);

    save |= sd_parse_movie(skel, ee, episode);

    return save;
}

static int sd_parse_multipart(
    epggrab_module_sd_t *skel,
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
    epggrab_module_sd_t *skel,
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
                save |= epg_broadcast_set_is_subtitled(ebc, 1, (epggrab_module_t *)&skel->mod);
            else if (strcmp(prop, "subtitled") == 0)
                save |= epg_broadcast_set_is_subtitled(ebc, 1, (epggrab_module_t *)&skel->mod);
            else if (strcmp(prop, "stereo") == 0)
                ;
            else if (strcmp(prop, "dvs") == 0)
                 save |= epg_broadcast_set_is_audio_desc(ebc, 1, (epggrab_module_t *)&skel->mod);
            else if (strcmp(prop, "DD 5.1") == 0)
                ;
            else
                printf("audio prop=%s\n", prop);
        }
    }

    return save;
}

static int sd_parse_video(
    epggrab_module_sd_t *skel,
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
                save |= epg_broadcast_set_is_hd(ebc, 1, (epggrab_module_t *)&skel->mod);
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
    epggrab_module_sd_t *skel,
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
        save |= epg_broadcast_set_is_new(ebc, 1, (epggrab_module_t *)&skel->mod);

    save |= sd_parse_multipart(skel, &epnum, program);

    save |= sd_parse_audio(skel, ebc, program);

    save |= sd_parse_video(skel, ebc, program);

    snprintf(uri, sizeof(uri)-1, "ddprogid://%s/%s", ((epggrab_module_t *)&skel->mod)->id, id);
    if (strncmp(id, "SH", 2) == 0 || strncmp(id, "EP", 2) == 0)
    {
        snprintf(suri, sizeof(suri)-1, "ddprogid://%s/%.10s", ((epggrab_module_t *)&skel->mod)->id, id);

        es = epg_serieslink_find_by_uri(suri, 1, &save2);
        if (es)
        {
            stats->seasons.total ++;
            if (save2) stats->seasons.created ++;

            save |= epg_broadcast_set_serieslink(ebc, es, (epggrab_module_t *)&skel->mod);
        }
    }

    ee = epg_episode_find_by_uri(uri, 1, &save3);
    if (ee)
    {
        stats->episodes.total ++;
        if (save3) stats->episodes.created ++;

        save |= epg_broadcast_set_episode(ebc, ee, (epggrab_module_t *)&skel->mod);

        save |= sd_parse_content_rating(skel, ee, program);

        if (episode)
            save |= process_episode(skel, ee, &epnum, id, episode);
    }

    if (save) stats->broadcasts.modified ++;
    if (save2) stats->seasons.modified ++;
    if (save3) stats->episodes.modified ++;

    return save | save2 | save3;
}

static void process_schedule_md5(
    epggrab_module_sd_t *skel,
    htsmsg_field_t *f,
    htsmsg_t *l,
    htsmsg_t *m5,
    int *cnt)
{
    htsmsg_t *m, *m2, *m3, *l2, *m4;
    htsmsg_field_t *f2;
    const char *station_id, *md5, *ch_md5;
    int save = 0;
    int day_cnt = 0;
    epggrab_channel_t *ch;

    if ((m = htsmsg_get_map_by_field(f)) == NULL)
        return;

    station_id = f->hmf_name;
    ch = epggrab_channel_find(skel->mod.channels,
        station_id, 0, &save,
        (epggrab_module_t *)&skel->mod);

    m4 = htsmsg_create_map();
    l2 = htsmsg_create_list();

    HTSMSG_FOREACH(f2, m)
    {
        if (skel->flush || !ch->md5) {
            htsmsg_add_str(l2, NULL, f2->hmf_name);
            day_cnt ++;
        }
        else
        {
            m2 = htsmsg_get_map_by_field(f2);
            md5 = htsmsg_get_str(m2, "md5");
            ch_md5 = htsmsg_get_str(ch->md5, f2->hmf_name);
            if (!ch_md5 || strcmp(ch_md5, md5) != 0) {
                htsmsg_add_str(l2, NULL, f2->hmf_name);
                day_cnt ++;
            }
            htsmsg_add_str(m4, f2->hmf_name, md5);
        }
    }
    htsmsg_add_msg(m5, station_id, m4);

    if (day_cnt > 0)
    {
        m3 = htsmsg_create_map();
        htsmsg_add_str(m3, "stationID", station_id);
        htsmsg_add_msg(m3, "date", l2);
        htsmsg_add_msg(l, NULL, m3);
        (*cnt) += day_cnt;
    }
    else
        htsmsg_destroy(l2);
}

static void process_schedule(
    epggrab_module_sd_t *skel,
    htsmsg_t *schedule,
    htsmsg_t *l,
    int *cnt)
{
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
                snprintf(uri, sizeof(uri)-1, "ddprogid://%s/%s", ((epggrab_module_t *)&skel->mod)->id, program_id);
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

htsmsg_t *sd_get_token(epggrab_module_sd_t *skel, CURL **curl, struct curl_slist **chunk)
{
    htsmsg_t *m = NULL;

    *curl = curl_easy_init();
    *chunk = curl_slist_append(NULL, "Content-Type: application/json;charset=UTF-8");

    curl_easy_setopt(*curl, CURLOPT_HTTPHEADER, *chunk);
    curl_easy_setopt(*curl, CURLOPT_ENCODING, "");
    curl_easy_setopt(*curl, CURLOPT_USERAGENT, USERAGENT);

    if (skel->node.username && skel->node.sha1_password)
        m = get_token(*curl, skel->node.username, skel->node.sha1_password);

    return m;
}

static void settings_save(epggrab_module_sd_t *skel)
{
    htsmsg_t *m = htsmsg_create_map();

    htsmsg_add_str(m, "username", skel->node.username);
    htsmsg_add_str(m, "password", skel->node.sha1_password);
    htsmsg_add_bool(m, "flush", skel->flush);
    htsmsg_add_u32(m, "update", skel->node.update);
    htsmsg_add_str(m, "uuid", idnode_uuid_as_str(&skel->node.sd_id));

    hts_settings_save(m, "epggrab/%s/config", ((epggrab_module_t *)&skel->mod)->id);

    htsmsg_destroy(m);
}

static char *_sd_grab(void *mod)
{
    epggrab_module_sd_t *skel = container_of(mod, epggrab_module_sd_t, mod);
    static char token[33];
    char *ret = NULL;
    const char *ptr;
    CURL *curl;
    struct curl_slist *chunk;
    int code = -1;
    htsmsg_t *m;

    m = sd_get_token(skel, &curl, &chunk);
    if (m)
    {
        if (!htsmsg_get_s32(m, "code", &code) && code == 0)
        {
            if ((ptr = htsmsg_get_str(m, "token")))
            {
                strcpy(token, ptr);
                ret = token;
            }
        }
        htsmsg_destroy(m);
    }

    curl_slist_free_all(chunk);
    curl_easy_cleanup(curl);

    return ret;
}


static int _sd_parse(
    void *mod,
    htsmsg_t *data,
    epggrab_stats_t *stats)
{
    epggrab_module_sd_t *skel = container_of(mod, epggrab_module_sd_t, mod);
    htsmsg_t *schedules, *episodes, *episode, *m, *programs, *program, *metadata, *md5s, *station_md5s;
    htsmsg_field_t *f, *f2;
    const char *station_id, *program_id;
    epggrab_channel_t *ch;
    epggrab_channel_link_t *ecl;
    int save = 0, save2 = 0;
    int code = -1;
    const char *md5, *start_date, *update;

    schedules = htsmsg_get_list(data, "schedules");
    episodes = htsmsg_get_map(data, "episodes");
    md5s = htsmsg_get_map(data, "md5s");
    update = htsmsg_get_str(data, "update");

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
                station_md5s = htsmsg_get_map(md5s, station_id);
                start_date = htsmsg_get_str(metadata, "startDate");
                htsmsg_set_str(station_md5s, start_date, md5);

                save = 0;
                ch = epggrab_channel_find(skel->mod.channels,
                    station_id, 0, &save,
                    (epggrab_module_t *)&skel->mod);
                stats->channels.total ++;
                save |= epggrab_channel_set_md5(ch, station_md5s);
                if (save)
                {
                    epggrab_channel_updated(ch);
                    stats->channels.modified ++;
                    save2 |= save;
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
                        save2 |= process_program(skel, ecl->ecl_channel, program, episode, stats);
                    }
                }
            }
        }
    }

    if (update)
    {
        skel->node.update = _sp_str2time(update);
        skel->flush = 0;
        settings_save(skel);
    }

    return save2;
}

static htsmsg_t *_sd_trans(void *mod, char *data)
{
    epggrab_module_sd_t *skel = container_of(mod, epggrab_module_sd_t, mod);
    CURL *curl;
    struct curl_slist *chunk = NULL;
    char token_header[40];
    const char *uri, *sid, *program_id, *channel, *update, *expiration;
    htsmsg_t *m, *v, *c, *m2, *v2, *c2, *c3, *m3, *s, *l, *n, *t, *msg, *ret, *m5, *eps, *account;
    htsmsg_t *lineups = NULL;
    int lineup_cnt = 0;
    htsmsg_field_t *f, *f2;
    uint32_t major, minor, freq;
    epggrab_channel_t *ch;
    char name[64];
    int save = 0;
    int cnt = 0;
    int i;
    htsmsg_t *logo;
    const char *url = NULL, __attribute__((unused)) *md5;
    int no_update = 0;

    if (data == NULL)
        return NULL;

    curl = curl_easy_init();
    chunk = curl_slist_append(chunk, "Content-Type: application/json;charset=UTF-8");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
    curl_easy_setopt(curl, CURLOPT_ENCODING, "");
    curl_easy_setopt(curl, CURLOPT_USERAGENT, USERAGENT);

    snprintf(token_header, sizeof(token_header), "Token: %s", data);
    chunk = curl_slist_append(chunk, token_header);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);

    if ((m = get_status(curl)) == NULL)
    {
        curl_slist_free_all(chunk);
        curl_easy_cleanup(curl);

        return NULL;
    }

    account = htsmsg_get_map(m, "account");
    if ((expiration = htsmsg_get_str(account, "expires")) != NULL)
        skel->node.expiration = _sp_str2time(expiration);
    if ((update = htsmsg_get_str(m, "lastDataUpdate")) != NULL)
    {
        if (_sp_str2time(update) == skel->node.update)
            no_update = 1;
    }

    v = htsmsg_get_list(m, "lineups");
    l = htsmsg_create_list();

    if (skel->lineups == NULL)
        lineups = htsmsg_create_map();

    HTSMSG_FOREACH(f, v)
    {
        c = htsmsg_get_map_by_field(f);
        uri = htsmsg_get_str(c, "uri");
        if (lineups)
        {
            skel->node.lineup[lineup_cnt] = lineup_cnt + 1;
            skel->server_lineup[lineup_cnt] = lineup_cnt + 1;
            m2 = htsmsg_create_map();
            htsmsg_add_u32(m2, "index", ++lineup_cnt);
            htsmsg_add_str(m2, "id", htsmsg_get_str(c, "lineup"));
            htsmsg_add_str(m2, "modified", htsmsg_get_str(c, "modified"));
            htsmsg_add_msg(lineups, uri, m2);
        }

        if (no_update)
            continue;

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
                channel = htsmsg_get_str(c2, "channel");
                htsmsg_get_u32(c2, "uhfVhf", &freq);
                htsmsg_get_u32(c2, "atscMajor", &major);
                htsmsg_get_u32(c2, "atscMinor", &minor);
                logo = htsmsg_get_map(c3, "logo");
                if (logo)
                {
                    url = htsmsg_get_str(logo, "URL");
                    md5 = htsmsg_get_str(logo, "md5");
//                    printf("%d-%d %s (%d) (%s)\n",
//                        major, minor, htsmsg_get_str(c3, "callsign"),
//                        freq, url);
                }
                else
                    url = NULL;

                save = 0;
                ch = epggrab_channel_find(skel->mod.channels,
                    sid, 1, &save,
                    (epggrab_module_t *)&skel->mod);
                if (channel)
                {
                    sprintf(name, "%s %s", channel,
                        htsmsg_get_str(c3, "callsign"));
                    if (sscanf(channel, "%d-%d", &major, &minor) == 2)
                        save |= epggrab_channel_set_number(ch, major, minor);
                    else
                        save |= epggrab_channel_set_number(ch, strtol(channel, NULL, 10), 0);
                }
                else
                {
                    sprintf(name, "%d-%d %s (%d)",
                        major, minor, htsmsg_get_str(c3, "callsign"), freq);
                    save |= epggrab_channel_set_number(ch, major, minor);
                }
                save |= epggrab_channel_set_name(ch, name);
                if (url)
                    save |= epggrab_channel_set_icon(ch, url);
                if (save)
                    epggrab_channel_updated(ch);

                if (LIST_FIRST(&ch->channels))
                {
                    n = htsmsg_create_map();
                    htsmsg_add_str(n, "stationID", sid);
                    htsmsg_add_msg(l, NULL, n);
                    cnt ++;
                }
            }
            htsmsg_destroy(m3);
            htsmsg_destroy(m2);
        }
    }

    if (skel->lineups == NULL)
        skel->lineups = lineups;

    ret = htsmsg_create_map();

    if (no_update)
        return ret;

    if (update != NULL)
        htsmsg_add_str(ret, "update", update);

    htsmsg_destroy(m);

    if (cnt > 0)
    {
        tvhlog(LOG_INFO, "sd", "Downloading %d schedule md5s\n", cnt);
        msg = get_schedules_md5(curl, l);

        cnt = 0;
        l = htsmsg_create_list();
        m5 = htsmsg_create_map();

        HTSMSG_FOREACH(f, msg)
        {
            process_schedule_md5(skel, f, l, m5, &cnt);
        }
        htsmsg_destroy(msg);

        htsmsg_add_msg(ret, "md5s", m5);
    }

    if (cnt > 0)
    {
        tvhlog(LOG_INFO, "sd", "Downloading %d days\n", cnt);
        msg = get_schedules(curl, l);
        cnt = 0;

        l = htsmsg_create_list();

        pthread_mutex_lock(&global_lock);

        HTSMSG_FOREACH(f, msg)
        {
            m = htsmsg_get_map_by_field(f);

            process_schedule(skel, m, l, &cnt);
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
                eps = get_episodes(curl, t);
                HTSMSG_FOREACH(f2, eps)
                {
                    m = htsmsg_detach_submsg(f2);
                    program_id = htsmsg_get_str(m, "programID");
                    htsmsg_add_msg(msg, program_id, m);
                }
                t = htsmsg_create_list();
                i = 0;
            }
            i++;
            program_id = htsmsg_field_get_str(f);
            htsmsg_add_str(t, 0, program_id);
        }
        eps = get_episodes(curl, t);
        HTSMSG_FOREACH(f2, eps)
        {
            m = htsmsg_detach_submsg(f2);
            program_id = htsmsg_get_str(m, "programID");
            htsmsg_add_msg(msg, program_id, m);
        }
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
    const char *uuid = NULL;

    curl_global_init(CURL_GLOBAL_DEFAULT);

    m = hts_settings_load("epggrab/sd/config");

    if (m)
    {
        skel->node.username = strdup(htsmsg_get_str(m, "username") ?: "");
        skel->node.sha1_password = strdup(htsmsg_get_str(m, "password") ?: "");
        uuid = htsmsg_get_str(m, "uuid");
        skel->flush = htsmsg_get_bool_or_default(m, "flush", 0);
        skel->node.update = htsmsg_get_u32_or_default(m, "update", dispatch_clock);
    }

    idnode_insert(&skel->node.sd_id, uuid, &epggrab_sd_device_class, 0);

    if (m)
        htsmsg_destroy(m);

    epggrab_module_int_create(&skel->mod, "sd", "Schedules Direct", 4, "sd",
            _sd_grab, _sd_parse, _sd_trans, &_sd_channels);
}

void sd_load(void)
{
    epggrab_module_channels_load(epggrab_module_find_by_id("sd"));
}