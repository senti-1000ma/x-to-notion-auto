import re
import time
import requests
import tweepy
import traceback
import streamlit as st
import streamlit.components.v1 as components
from notion_client import Client, APIResponseError

st.set_page_config(page_title="X → Notion Sync", page_icon="🐴", layout="centered")
st.title("🐴 X → Notion Sync By. 1000ma")
st.caption("각자 본인 키와 DB ID만 입력하면 ‘조회수/좋아요’를 노션 DB에 채워 넣습니다. 배치는 100개씩 처리합니다.")
st.link_button("🩵 1000ma 팔로우로 응원하기", "https://x.com/o000oo0o0o00", use_container_width=True)

st.sidebar.link_button("🩵 1000ma 팔로우로 응원하기", "https://x.com/o000oo0o0o00", use_container_width=True)

with st.form("config"):
    st.subheader("🔐 입력값")
    st.write("※ 공개 저장소/로그에 토큰이 남지 않도록 주의하세요. (이 앱은 입력값을 서버에 저장하지 않습니다)")
    col1, col2 = st.columns(2)
    with col1:
        x_token = st.text_input("X Bearer Token", value=st.secrets.get("X_BEARER_TOKEN", ""), type="password")
        db_id = st.text_input("Notion Database ID", value=st.secrets.get("NOTION_DATABASE_ID", ""))
    with col2:
        notion_token = st.text_input("Notion Token", value=st.secrets.get("NOTION_TOKEN", ""), type="password")
    st.subheader("🧱 노션 컬럼 이름 (읽기 전용)")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.text("URL 컬럼: x.com Link")
        prop_url = "x.com Link"
    with c2:
        st.text("조회수 컬럼: Views on X")
        prop_views = "Views on X"
    with c3:
        st.text("좋아요 컬럼: Likes")
        prop_likes = "Likes"
    st.subheader("⚙️ 옵션")
    opt_overwrite = st.checkbox("이미 값 있어도 덮어쓰기", value=True)
    batch_sleep = st.number_input("배치 사이 대기(초)", min_value=0.0, max_value=5.0, value=1.0, step=0.1)
    submitted = st.form_submit_button("🚀 실행")

TWEET_RE = re.compile(
    r"https?://(?:www\.)?(?:x|twitter)\.com/(?:i/web/)?status/(\d+)"
    r"|https?://(?:www\.)?(?:x|twitter)\.com/[\w\d\-_]+/status/(\d+)"
)

def extract_tweet_id(url: str):
    if not url:
        return None
    m = TWEET_RE.search(url)
    if m:
        return m.group(1) or m.group(2)
    try:
        r = requests.get(url, allow_redirects=True, timeout=8)
        m2 = TWEET_RE.search(r.url)
        if m2:
            return m2.group(1) or m2.group(2)
    except Exception:
        pass
    return None

def chunked(iterable, size):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def read_url_from_row(row: dict, prop_name: str) -> str | None:
    props = row.get("properties", {})
    p = props.get(prop_name)
    if not p:
        return None
    if p.get("type") == "url":
        return p.get("url")
    if p.get("type") == "rich_text":
        rts = p.get("rich_text", [])
        for rt in rts:
            href = rt.get("href")
            if href:
                return href
            if rt.get("type") == "text":
                content = rt["text"].get("content", "")
                if content.startswith("http"):
                    return content
    return None

def read_number(row: dict, prop_name: str):
    props = row.get("properties", {})
    p = props.get(prop_name)
    if p and p.get("type") == "number":
        return p.get("number")
    return None

def query_database_all(notion: Client, database_id: str):
    start_cursor = None
    while True:
        payload = {"database_id": database_id, "page_size": 100}
        if start_cursor:
            payload["start_cursor"] = start_cursor
        resp = notion.databases.query(**payload)
        for row in resp.get("results", []):
            yield row
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")

def js_safe(s: str) -> str:
    return (s or "").replace("\\", "\\\\").replace("`", "\\`")

if submitted:
    if not (x_token and notion_token and db_id):
        st.error("X 토큰, Notion 토큰, DB ID를 모두 입력해 주세요.")
        st.stop()

    try:
        x_client = tweepy.Client(bearer_token=x_token, wait_on_rate_limit=False)
        notion = Client(auth=notion_token)
    except Exception as e:
        st.error("초기화 실패 · 1000ma에게 로그를 보내주세요")
        err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        st.code(err, language="text")
        components.html(
            f"""
            <div style="display:flex;gap:8px;justify-content:flex-start">
              <button onclick="navigator.clipboard.writeText(`{js_safe(err)}`)" style="padding:.5rem 1rem;">로그 복사</button>
            </div>
            """,
            height=50,
        )
        st.stop()

    with st.status("🔎 Notion DB 확인 중...", expanded=False) as s:
        try:
            db = notion.databases.retrieve(database_id=db_id)
            db_title = "".join([t.get("plain_text","") for t in db.get("title", [])]) or "(제목 없음)"
            st.write(f"DB: **{db_title}**")
            s.update(label="✅ Notion DB 연결 OK", state="complete")
        except Exception as e:
            s.update(label="❌ Notion DB 연결 실패", state="error")
            st.error("DB 연결 실패 · 1000ma에게 로그를 보내주세요")
            err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            st.code(err, language="text")
            components.html(
                f"""
                <div style="display:flex;gap:8px;justify-content:flex-start">
                  <button onclick="navigator.clipboard.writeText(`{js_safe(err)}`)" style="padding:.5rem 1rem;">로그 복사</button>
                </div>
                """,
                height=50,
            )
            st.stop()

    st.subheader("1) 트윗 링크 수집")
    try:
        rows = list(query_database_all(notion, db_id))
    except Exception as e:
        st.error("페이지 조회 실패 · 1000ma에게 로그를 보내주세요")
        err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        st.code(err, language="text")
        components.html(
            f"""
            <div style="display:flex;gap:8px;justify-content:flex-start">
              <button onclick="navigator.clipboard.writeText(`{js_safe(err)}`)" style="padding:.5rem 1rem;">로그 복사</button>
            </div>
            """,
            height=50,
        )
        st.stop()

    total_rows = len(rows)
    st.write(f"총 {total_rows}행 탐색 중…")

    pairs = []
    skipped_no_url, skipped_no_id, skipped_existing = 0, 0, 0
    prog = st.progress(0)
    for i, row in enumerate(rows, start=1):
        page_id = row["id"]
        url = read_url_from_row(row, prop_url)
        if not url:
            skipped_no_url += 1
        else:
            if not opt_overwrite:
                v_now = read_number(row, prop_views)
                l_now = read_number(row, prop_likes)
                if (v_now is not None) and (l_now is not None):
                    skipped_existing += 1
                    prog.progress(i / total_rows)
                    continue
            tid = extract_tweet_id(url)
            if not tid:
                skipped_no_id += 1
            else:
                pairs.append((page_id, tid))
        prog.progress(i / total_rows)

    if not pairs:
        st.error("처리할 트윗이 없습니다. (URL/ID 미검출 or 모두 스킵)")
    else:
        st.success(f"수집 완료: {len(pairs)}개 (URL 없음 {skipped_no_url}, ID 실패 {skipped_no_id}, 기존값 스킵 {skipped_existing})")

    if not pairs:
        st.stop()

    st.subheader("2) 배치 조회 & 업데이트")
    updated, failed, miss = 0, 0, 0
    log_area = st.empty()

    for batch_idx, batch in enumerate(chunked(pairs, 100), start=1):
        id_list = [tid for _, tid in batch]
        log_area.write(f"배치 {batch_idx}: {len(id_list)}개 조회 중…")
        try:
            resp = x_client.get_tweets(ids=id_list, tweet_fields=["public_metrics"])
        except tweepy.TooManyRequests as e:
            st.error("X API 사용 횟수 초과입니다. 쿼터가 리셋될 때까지 기다려야 합니다.")
            try:
                st.code(e.response.text, language="json")
            except Exception:
                st.code(str(e), language="text")
            st.stop()
        except Exception as e:
            st.error("예기치 못한 에러가 발생했습니다. 에러 발생 · 1000ma에게 로그를 보내주세요")
            err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            st.code(err, language="text")
            components.html(
                f"""
                <div style="display:flex;gap:8px;justify-content:flex-start">
                  <button onclick="navigator.clipboard.writeText(`{js_safe(err)}`)" style="padding:.5rem 1rem;">로그 복사</button>
                </div>
                """,
                height=50,
            )
            st.stop()

        if getattr(resp, "errors", None):
            not_found_errors = [e for e in resp.errors if e.get('title') == 'Not Found Error']
            
            if not_found_errors:
                error_ids = [e.get('resource_id') for e in not_found_errors if e.get('resource_id')]
                error_id_str = ", ".join(error_ids)
                
                st.error(
                    f"🚨 **트윗 찾기 실패 ({len(error_ids)}건):** 삭제되었거나 비공개 트윗이 있습니다. "
                    f"Notion DB에서 다음 ID(들)의 링크를 확인(삭제/URL 제거) 후 다시 시도해 주세요.\n\n"
                    f"`{error_id_str}`"
                )
                st.stop()
            else:
                st.error("X API 응답에 에러가 포함되어 있습니다. 사용 횟수 초과일 수 있으며, 쿼터 리셋을 기다려야 합니다.")
                st.code(str(resp.errors), language="json")
                st.stop()

        metrics_map = {}
        if resp and resp.data:
            for tw in resp.data:
                pm = getattr(tw, "public_metrics", {}) or {}
                likes = pm.get("like_count")
                views = pm.get("impression_count") if "impression_count" in pm else None
                metrics_map[str(tw.id)] = (views, likes)

        if not metrics_map:
            st.warning("응답에 메트릭이 비어 있습니다. 아래 원시 응답을 확인하세요.")
            try:
                st.code(repr(resp.data) + "\n\nmeta=" + repr(getattr(resp, "meta", None)), language="text")
            except Exception:
                st.code(str(resp), language="text")
            st.stop()

        for page_id, tid in batch:
            if tid not in metrics_map:
                miss += 1
                continue
            views, likes = metrics_map[tid]
            props_update = {}
            if views is not None:
                props_update[prop_views] = {"number": float(views)}
            if likes is not None:
                props_update[prop_likes] = {"number": float(likes)}
            if not props_update:
                continue
            try:
                notion.pages.update(page_id=page_id, properties=props_update)
                updated += 1
            except APIResponseError as e:
                st.error("Notion 업데이트 실패 · 1000ma에게 로그를 보내주세요")
                err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                st.code(err, language="text")
                components.html(
                    f"""
                    <div style="display:flex;gap:8px;justify-content:flex-start">
                      <button onclick="navigator.clipboard.writeText(`{js_safe(err)}`)" style="padding:.5rem 1rem;">로그 복사</button>
                    </div>
                    """,
                    height=50,
                )
                st.stop()
            except Exception as e:
                st.error("알 수 없는 업데이트 실패 · 1000ma에게 로그를 보내주세요")
                err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                st.code(err, language="text")
                components.html(
                    f"""
                    <div style="display:flex;gap:8px;justify-content:flex-start">
                      <button onclick="navigator.clipboard.writeText(`{js_safe(err)}`)" style="padding:.5rem 1rem;">로그 복사</button>
                    </div>
                    """,
                    height=50,
                )
                st.stop()

        time.sleep(batch_sleep)

    st.success(
        f"✅ 완료: 업데이트 {updated}건, 실패 {failed}건, 응답 누락 {miss}건 "
        f"(URL 없음 {skipped_no_url}, ID 실패 {skipped_no_id}, 기존값 스킵 {skipped_existing})"
    )