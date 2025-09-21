import re
import time
import requests
import tweepy
import streamlit as st
from notion_client import Client

st.set_page_config(page_title="X → Notion Sync", page_icon="🐴", layout="centered")
st.title("🐴 X → Notion Sync By. 1000ma")
st.caption("각자 본인 키와 DB ID만 입력하면 ‘조회수/좋아요’를 노션 DB에 채워 넣고, 타이틀에 #1, #2… 시리얼 번호를 부여합니다. 배치는 100개씩 처리합니다.")
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

    st.subheader("🔢 시리얼(타이틀) 리넘버링")
    do_renumber = st.checkbox("타이틀을 #1, #2…로 자동 리넘버링", value=True)
    renumber_overwrite = st.checkbox("타이틀에 기존 값 있어도 덮어쓰기", value=True)
    order_choice = st.selectbox(
        "리넘버링 순서",
        ("생성일 오래된→최신", "생성일 최신→오래된", "현재 정렬 유지"),
        index=0
    )

    st.subheader("⚙️ X → Notion 동기화 옵션")
    opt_overwrite = st.checkbox("조회수/좋아요 기존 값 있어도 덮어쓰기", value=True)
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
    """Notion row에서 URL 속성(우선), 없으면 rich_text에서 URL 추출"""
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

if submitted:
    if not (x_token and notion_token and db_id):
        st.error("X 토큰, Notion 토큰, DB ID를 모두 입력해 주세요.")
        st.stop()

    x_client = tweepy.Client(bearer_token=x_token, wait_on_rate_limit=True)
    notion = Client(auth=notion_token)

    with st.status("🔎 Notion DB 확인 중...", expanded=False) as s:
        try:
            db = notion.databases.retrieve(database_id=db_id)
            db_title = "".join([t.get("plain_text","") for t in db.get("title", [])]) or "(제목 없음)"
            st.write(f"DB: **{db_title}**")
            db_props = db.get("properties", {})

            title_prop_name = None
            for k, v in db_props.items():
                if v.get("type") == "title":
                    title_prop_name = k
                    break

            if not title_prop_name:
                s.update(label="❌ 타이틀 속성을 찾지 못했습니다.", state="error")
                st.stop()

            s.update(label="✅ Notion DB 연결 OK", state="complete")
        except Exception as e:
            s.update(label="❌ Notion DB 연결 실패", state="error")
            st.exception(e)
            st.stop()

    st.subheader("1) 행 수집")
    rows = list(query_database_all(notion, db_id))
    total_rows = len(rows)
    st.write(f"총 {total_rows}행 탐색 중…")

    if do_renumber:
        st.subheader("1-α) 타이틀 리넘버링 (#1, #2, …)")
        if order_choice == "생성일 오래된→최신":
            rows_for_serial = sorted(rows, key=lambda r: r.get("created_time", ""))
        elif order_choice == "생성일 최신→오래된":
            rows_for_serial = sorted(rows, key=lambda r: r.get("created_time", ""), reverse=True)
        else:
            rows_for_serial = rows  # 현재 순서 유지

        serial_updated = 0
        serial_skipped = 0
        serial_failed = 0

        prog_serial = st.progress(0.0)
        for i, row in enumerate(rows_for_serial, start=1):
            page_id = row["id"]

            cur_title_blocks = row.get("properties", {}).get(title_prop_name, {}).get("title", [])
            cur_title_text = "".join([b.get("plain_text", "") for b in cur_title_blocks]) if cur_title_blocks else ""
            label = f"#{i}"

            if (not renumber_overwrite) and cur_title_text.strip():
                serial_skipped += 1
                prog_serial.progress(i / len(rows_for_serial))
                continue

            try:
                notion.pages.update(
                    page_id=page_id,
                    properties={
                        title_prop_name: {
                            "title": [
                                { "type": "text", "text": { "content": label } }
                            ]
                        }
                    }
                )
                serial_updated += 1
            except Exception as e:
                serial_failed += 1
                st.write(f"[ERR] Serial(title) update {page_id[:8]}…: {e}")

            prog_serial.progress(i / len(rows_for_serial))

        st.success(f"리넘버링 완료: 업데이트 {serial_updated}건, 스킵 {serial_skipped}건, 실패 {serial_failed}건")

    st.subheader("2) 트윗 링크 수집")
    pairs = []
    skipped_no_url, skipped_no_id, skipped_existing = 0, 0, 0

    prog = st.progress(0.0)
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
        st.stop()

    st.success(f"수집 완료: {len(pairs)}개 (URL 없음 {skipped_no_url}, ID 실패 {skipped_no_id}, 기존값 스킵 {skipped_existing})")

    st.subheader("3) 배치 조회 & 업데이트")
    updated, failed, miss = 0, 0, 0
    log_area = st.empty()

    for batch_idx, batch in enumerate(chunked(pairs, 100), start=1):
        id_list = [tid for _, tid in batch]
        log_area.write(f"배치 {batch_idx}: {len(id_list)}개 조회 중…")

        try:
            resp = x_client.get_tweets(ids=id_list, tweet_fields=["public_metrics"])
        except Exception as e:
            st.error(f"[ERR] 배치 {batch_idx} 요청 실패: {e}")
            failed += len(batch)
            time.sleep(batch_sleep)
            continue

        metrics_map = {}
        if resp and resp.data:
            for tw in resp.data:
                pm = getattr(tw, "public_metrics", {}) or {}
                likes = pm.get("like_count")
                views = pm.get("impression_count") if "impression_count" in pm else None
                metrics_map[str(tw.id)] = (views, likes)

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
            except Exception as e:
                failed += 1
                st.write(f"[ERR] Notion update {tid}: {e}")

        time.sleep(batch_sleep)

    st.success(
        f"✅ 완료: 업데이트 {updated}건, 실패 {failed}건, 응답 누락 {miss}건 "
        f"(URL 없음 {skipped_no_url}, ID 실패 {skipped_no_id}, 기존값 스킵 {skipped_existing})"
    )
    st.info("참고: `impression_count`(조회수)는 X API 플랜/권한에 따라 제공되지 않을 수 있습니다. 그 경우 조회수는 비워둡니다.")
