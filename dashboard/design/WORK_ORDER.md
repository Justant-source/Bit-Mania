# Bit-Mania Dashboard — Design System 마이그레이션 작업지시서

> **대상**: Claude Code (또는 후속 작업자)
> **목적**: 기존 `dashboard/src/public/` 의 GitHub-dark 스타일을 `dashboard/design/index.html` 에 정의된 새 디자인 시스템으로 일관성 있게 교체한다.
> **단일 진실 공급원 (SSOT)**: [`dashboard/design/index.html`](./index.html)

---

## 0. 작업 원칙

1. **SSOT 우선**: 색·간격·타입·컴포넌트의 모든 결정은 `dashboard/design/index.html` 의 `<style>` 블록을 따른다. 새 값을 만들면 안 된다 — 토큰에 없으면 먼저 SSOT에 추가하고 그 다음 코드에 쓴다.
2. **무중단 운영 유지**: Phase 5 메인넷 실전 중. 대시보드 변경이 서버 API·DB·Redis 컨트랙트를 건드리면 안 된다. **수정 범위는 `dashboard/src/public/` (HTML + CSS + JS) 와 새로 만드는 CSS 파일에 한정**.
3. **기능 동등성**: 기존 화면이 보여주던 모든 정보·인터랙션은 그대로 유지한다. 데이터 fetch 로직 (`monitor-dashboard.js`, `supertrend-dashboard.js`) 의 입출력은 변경 금지, **렌더링 출력의 HTML 마크업·클래스명만 교체**.
4. **두 테마 지원**: 라이트가 기본. 다크는 `<html data-theme="dark">` 한 줄로 동작해야 한다.
5. **문서 동기화**: `CLAUDE.md` 규칙대로 `docs/CODE_MAP.md` 에 새 파일 경로를 등록한다.

---

## 1. 산출물 (Definition of Done)

작업 완료 시 다음이 충족되어야 한다.

- [ ] `dashboard/src/public/css/tokens.css` 신규 — SSOT 의 TOKENS 블록 그대로
- [ ] `dashboard/src/public/css/components.css` 신규 — SSOT 의 COMPONENTS 블록 그대로
- [ ] `dashboard/src/public/css/dashboard.css` 갱신 — 페이지 특수 레이아웃만 남기고 슬림화 (현재 9.3KB → 목표 < 3KB)
- [ ] `dashboard/src/public/monitor.html` 갱신 — 새 컴포넌트로 마크업 재구성
- [ ] `dashboard/src/public/supertrend.html` 갱신 — 동일
- [ ] `dashboard/src/public/js/monitor-dashboard.js` — Plotly 차트가 토큰 색을 읽도록 패치
- [ ] `dashboard/src/public/js/supertrend-dashboard.js` — 동일
- [ ] `dashboard/src/public/js/theme.js` 신규 — 라이트/다크 토글 + localStorage
- [ ] `docs/CODE_MAP.md` 에 위 파일들 추가
- [ ] 두 화면 모두 라이트·다크 양쪽에서 렌더 깨짐 없이 동작

---

## 2. 파일 분리 — Step 1 (선행 작업)

`dashboard/design/index.html` 의 `<style>` 블록을 그대로 두 파일로 잘라낸다. 변수명·셀렉터·값은 **한 글자도 바꾸지 않는다**.

### 2.1 `tokens.css` 추출 범위

```
/* =========================================================================
   1. DESIGN TOKENS
   ...
   ========================================================================= */
:root { ... }
[data-theme="dark"] { ... }
```

추가로 base reset 도 같이 넣는다 (`*` 박싱, `body`, `a`, `button`, 스크롤바).

### 2.2 `components.css` 추출 범위

```
/* =========================================================================
   4. COMPONENTS — production primitives.
   ...
   ========================================================================= */
```

부터 파일 끝의 유틸리티 클래스 (`.flex`, `.muted`, `.mono`, `.pos`, `.neg`, `.warn`) 까지.

### 2.3 `dashboard.css` 슬림화

기존 파일에서 다음만 남기고 모두 삭제:

- `.app` `.main` 외 페이지 그리드 (단, 새 `app-shell` 패턴으로 교체할 거면 이것도 제거)
- 기존 `.cmp-*` 컴페어 테이블 색은 토큰 기반 `.pos / .neg / .warn` 으로 대체
- 페이지-한정 미세 조정 (예: `#chart-price` 의 최소 높이) 만 남김

`dashboard.css` 가 `tokens.css`, `components.css` 뒤에 로드되도록 HTML import 순서 확인.

---

## 3. HTML 마크업 마이그레이션 — 클래스 매핑표

기존 클래스 → 새 클래스. 동일하게 두면 안 되는 항목은 **반드시 교체**.

| 기존                          | 신규                          | 비고                                                  |
| ----------------------------- | ----------------------------- | ----------------------------------------------------- |
| `.app`                        | `.app-shell`                  | grid 구조 변경 (240px 사이드바 + topbar + page)       |
| `.sidebar` 내부 인라인 스타일 | `.brand` `.strategy-card` `.nav` `.nav-section` `.nav-item` `.sidebar-footer` | SSOT 컴포넌트 사용 — 인라인 `style=...` 모두 제거 |
| `.nav-icon` (이모지 📊 🖥)    | `.nav-icon` + 단순 글리프(▤ ⌖) 또는 [Tabler Icons](https://tabler-icons.io) inline SVG | **이모지 제거** — 디자인 시스템 원칙          |
| (없음) topbar                 | `.topbar` `.crumbs` `.env-pill` `.topbar-actions` | 메인넷/Phase 5 표시는 `.env-pill` 로                  |
| `.kpi-grid` `.kpi-card` `.kpi-label` `.kpi-value` `.kpi-sub` | `.row.row-6` `.kpi` `.kpi-label` `.kpi-value` `.kpi-meta` | 동일하지만 컬럼 수는 `row-6` 명시                     |
| `.section-hdr` (선-텍스트-선) | `.section-bar` (h3 + meta)    | 라인 장식 제거, 좌측 정렬 + 우측 메타                 |
| `.chart-card`                 | `.card` + `.card-head`        | 패딩·라디우스 토큰 기반                               |
| `.card-title`                 | `.card-title` (그대로) + `.card-sub` | 부제는 `.card-sub`                              |
| `.badge-green/red/yellow/blue/gray` | `.badge.success/.danger/.warning/.info/.neutral` | 의미 기반 네이밍                                |
| `.health-grid` `.health-card` `.health-dot.green` | `.row.row-3` `.health-card` `.status-dot.green` (선택적 `.live` 클래스로 펄스) | 마크업 거의 동일                              |
| `.gauge-grid` `.gauge-card`   | (SSOT 의 인프라 카드 패턴)    | 4 또는 6 컬럼 grid                                    |
| `.tbl-wrap` `table`           | `.tbl-wrap` `table.tbl`       | 헤더에 `.tbl thead th` 자동 스타일                    |
| `.modal-overlay` `.modal-box` | (재사용 — 색만 토큰 교체)     | 별도 컴포넌트화 필요 시 SSOT 에 먼저 추가             |
| `.day-btn` (supertrend.html 의 인라인 스타일) | `.btn-group` + `.btn.sm` + `.active` | 인라인 `<style>` 블록 제거                  |
| 인라인 `style="color:#xxx"`   | 토큰 변수 또는 `.pos/.neg/.warn/.muted` | **모든 인라인 색 하드코딩 제거**           |

### 3.1 페이지 구조 템플릿

```html
<body>
  <div class="app-shell" data-screen-label="01 Monitor">
    <aside class="sidebar"> ... </aside>
    <header class="topbar"> ... </header>
    <main class="page">
      <!-- 페이지 콘텐츠 -->
    </main>
  </div>
</body>
```

> `data-screen-label` 은 코멘트/내비게이션 컨텍스트용. `01 Monitor`, `02 Supertrend` 형식.

---

## 4. Plotly 차트 — 토큰 연동

기존 코드는 `'#3fb950'` 같은 hex 를 Plotly trace 에 직접 넣는다. 토큰을 읽도록 패치한다.

### 4.1 헬퍼 추가 (각 dashboard JS 파일 상단)

```js
const T = (name) => getComputedStyle(document.documentElement)
                      .getPropertyValue(name).trim();

const palette = () => ({
  success: T('--c-success'),
  danger:  T('--c-danger'),
  warning: T('--c-warning'),
  info:    T('--c-info'),
  text:    T('--text'),
  muted:   T('--text-muted'),
  border:  T('--border'),
  surface: T('--surface'),
  bg:      T('--bg'),
});

const plotlyLayout = () => {
  const p = palette();
  return {
    paper_bgcolor: 'transparent',
    plot_bgcolor:  'transparent',
    font: { family: "'Inter', system-ui, sans-serif", color: p.text, size: 12 },
    xaxis: { gridcolor: p.border, linecolor: p.border, tickcolor: p.border, zerolinecolor: p.border },
    yaxis: { gridcolor: p.border, linecolor: p.border, tickcolor: p.border, zerolinecolor: p.border },
    margin: { l: 48, r: 16, t: 16, b: 36 },
    showlegend: true,
    legend: { bgcolor: 'transparent', font: { color: p.muted, size: 11 } },
  };
};
```

### 4.2 trace 색 교체

| 의미       | 기존        | 신규               |
| ---------- | ----------- | ------------------ |
| 이익/실제  | `#3fb950`   | `palette().success` |
| 손실/Kill  | `#f85149`   | `palette().danger`  |
| 주의/슬리피지 | `#e3b341` | `palette().warning` |
| 예상/참고  | `#1f6feb` / `#79c0ff` | `palette().info` |
| 중립       | `#8b949e`   | `palette().muted`   |

### 4.3 테마 변경 시 차트 리렌더

`theme.js` 의 토글 콜백에서 `Plotly.relayout(elementId, plotlyLayout())` 를 호출해 색을 다시 입힌다.

---

## 5. 테마 토글 (`theme.js`)

```js
(function () {
  const KEY = 'bm-theme';
  const root = document.documentElement;
  const apply = (t) => {
    root.setAttribute('data-theme', t);
    try { localStorage.setItem(KEY, t); } catch (e) {}
    // 차트 리렌더 트리거
    window.dispatchEvent(new CustomEvent('bm:themechange', { detail: t }));
  };
  const saved = (() => { try { return localStorage.getItem(KEY); } catch (e) { return null; } })()
              || (matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
  apply(saved);

  // 토글 버튼 (topbar 의 .theme-toggle) 위임
  document.addEventListener('click', (e) => {
    const btn = e.target.closest('[data-set-theme]');
    if (btn) apply(btn.dataset.setTheme);
  });
})();
```

`monitor.html` · `supertrend.html` 의 topbar 우측에 다음 토글을 둔다:

```html
<div class="theme-toggle">
  <button data-set-theme="light">☀︎</button>
  <button data-set-theme="dark">☾</button>
</div>
```

---

## 6. 작업 순서 (권장)

1. **(P0)** `tokens.css` + `components.css` 추출 → 두 HTML 에서 import
2. **(P0)** `monitor.html` 마크업 교체 → 라이트 모드 시각 확인
3. **(P0)** `supertrend.html` 마크업 교체 → 라이트 모드 시각 확인
4. **(P1)** `theme.js` 추가 → `data-theme="dark"` 토글 확인
5. **(P1)** Plotly trace 색을 토큰 기반으로 패치
6. **(P1)** `dashboard.css` 슬림화 + 잔여물 정리
7. **(P2)** 이모지 → 글리프 또는 SVG 아이콘 교체
8. **(P2)** `docs/CODE_MAP.md` 갱신 + 작업 완료 커밋

---

## 7. 검증 체크리스트

배포 전 다음을 모두 통과해야 한다.

### 7.1 시각

- [ ] 라이트 모드 두 화면 모두 `#F6F8FB` 배경, 흰 카드, 4–8px 라디우스로 렌더
- [ ] 다크 모드 토글 시 같은 컴포넌트가 `#0F1623` 베이스로 자연스럽게 전환
- [ ] 어디에도 `#0d1117` `#161b22` `#1f6feb` 같은 raw hex 가 남아 있지 않음 (`grep -rE '#[0-9a-fA-F]{6}' dashboard/src/public/` 으로 확인)
- [ ] 모든 이모지 (📊 🖥 ✅ ❌ ⚠️) 가 디자인 시스템 컴포넌트로 대체됨
- [ ] 숫자 컬럼이 우측 정렬 + tabular-nums (Inter + JetBrains Mono)

### 7.2 기능

- [ ] `monitor.html` 의 KPI 6개 / 자산 곡선 / 포지션 / Kill 이력 / 헬스 / 인프라 게이지 모두 데이터 표시
- [ ] `supertrend.html` 의 가격 차트 / 자산 곡선 / 비교 테이블 / 지표 패널 모두 표시
- [ ] 차트 클릭→24h 추이 모달 정상 동작
- [ ] 사이드바 활성 표시가 현재 페이지에 맞게 표시
- [ ] 마지막 갱신 시각이 실시간으로 업데이트

### 7.3 코드 품질

- [ ] 두 HTML 모두 `<style>` 인라인 블록 없음 (`day-btn` 같은 잔재 제거)
- [ ] 인라인 `style="..."` 으로 색·간격을 지정한 곳이 없음
- [ ] CSS 변수 외 raw hex 색을 코드에서 검색해 모두 제거 (예외: Plotly trace 는 헬퍼 경유 OK)
- [ ] `theme.js` 가 새로고침 후에도 마지막 테마 복원

---

## 8. 범위 외 (Out of Scope)

다음은 이번 작업에서 **하지 않는다**.

- 서버 라우트 (`dashboard/src/routes/*.ts`) 의 응답 스키마 변경
- Redis pub/sub 채널·메시지 형식 변경
- `cryptoengine/` 서비스 코드 (전략·실행·Kill Switch) 의 어떠한 변경도
- 새 기능 추가 (백테스트 화면, 설정 화면 등) — 디자인 시스템이 준비됐다는 것만 보여줌
- Plotly 외 차트 라이브러리 교체

새 화면이나 새 컴포넌트가 필요해지면 **먼저 `dashboard/design/index.html` 에 컴포넌트를 추가**한 뒤 후속 PR 로 진행한다.

---

## 9. 참고

- SSOT: [`dashboard/design/index.html`](./index.html) — 토큰·컴포넌트·적용 예시 모두 포함
- 토큰 명세: SSOT 의 `Section 1. DESIGN TOKENS` 블록
- 컴포넌트 카탈로그: SSOT 의 `02 · Library` 섹션
- 적용 예시 (목표 모습): SSOT 의 `03 · Applied · Monitor` / `Supertrend` 섹션
- 운영 규칙: [`/CLAUDE.md`](../../CLAUDE.md) — 문서 동기화·메인넷 신중 운영
