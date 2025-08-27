# 🏥 GC 내시경실 관리시스템 - 웹 애플리케이션 버전

기존 Streamlit 기반 시스템을 현대적인 웹 애플리케이션으로 변환한 버전입니다.

## 🌟 주요 특징

- **현대적인 UI/UX**: 반응형 디자인으로 모든 디바이스에서 최적화
- **실시간 업데이트**: 즉시 반영되는 스케줄 및 방 배정 정보
- **사용자 친화적**: 직관적인 인터페이스로 쉽고 빠른 조작
- **관리자 대시보드**: 강력한 관리 기능과 리포트 생성
- **모바일 지원**: 스마트폰과 태블릿에서도 완벽하게 작동

## 🚀 빠른 시작

### 1. 의존성 설치
```bash
npm install
```

### 2. 서버 시작
```bash
# 개발 모드
npm run dev

# 프로덕션 모드
npm start

# PM2를 사용한 데몬 모드
pm2 start ecosystem.config.js
```

### 3. 웹 브라우저에서 접속
```
http://localhost:3000
```

## 👥 테스트 계정

### 일반 사용자 계정
- **의사**: `001` / `user123`
- **간호사**: `002` / `user123`  
- **기사**: `003` / `user123`

### 관리자 계정
- **관리자**: `admin` / `admin123`

## 📋 주요 기능

### 🔐 사용자 기능
- **로그인/로그아웃**: 사번 기반 인증 시스템
- **개인 대시보드**: 내 근무 현황 한눈에 보기
- **스케줄 관리**: 개인 마스터 스케줄 확인 및 수정
- **요청사항 제출**: 휴가, 스케줄 변경, 방 배정 요청
- **실시간 알림**: 요청 처리 상태 즉시 확인

### 👨‍💼 관리자 기능
- **직원 관리**: 직원 추가/수정/삭제
- **자동 스케줄 배정**: 효율적인 근무 스케줄 자동 생성
- **방 배정 관리**: 실시간 방 상태 관리
- **리포트 생성**: 월별 근무 리포트 및 통계
- **Excel 다운로드**: 스케줄 데이터 Excel 내보내기

## 🏗️ 시스템 구조

```
web-app/
├── index.html          # 메인 페이지
├── css/
│   └── style.css       # 스타일시트
├── js/
│   └── app.js          # 프론트엔드 로직
├── api/
│   └── server.js       # 백엔드 API 서버
├── images/             # 이미지 파일들
├── package.json        # 의존성 정보
├── ecosystem.config.js # PM2 설정
└── README.md          # 이 파일
```

## 🔧 기술 스택

### Frontend
- **HTML5**: 시맨틱 마크업
- **CSS3**: Flexbox, Grid, 애니메이션
- **JavaScript**: ES6+ 모던 문법
- **Font Awesome**: 아이콘
- **Google Fonts**: 한글 폰트 (Noto Sans KR)

### Backend
- **Node.js**: 런타임 환경
- **Express.js**: 웹 프레임워크
- **CORS**: 크로스 오리진 요청 처리
- **PM2**: 프로세스 관리

### 배포 및 운영
- **PM2**: 무중단 서비스 운영
- **로그 관리**: 에러 및 액세스 로그
- **환경 변수**: 개발/프로덕션 환경 분리

## 📱 반응형 디자인

### 데스크톱 (1200px+)
- 전체 기능 사용 가능
- 넓은 화면을 활용한 효율적 레이아웃

### 태블릿 (768px ~ 1199px)
- 터치 친화적 인터페이스
- 적절한 버튼 크기 및 간격

### 모바일 (768px 미만)
- 모바일 최적화 네비게이션
- 세로 스크롤 중심의 레이아웃
- 터치 제스처 지원

## 🔒 보안 기능

- **사용자 인증**: 사번 기반 로그인 시스템
- **권한 관리**: 역할별 접근 권한 제어
- **세션 관리**: 자동 로그아웃 및 토큰 관리
- **데이터 검증**: 입력 데이터 유효성 검사

## 📊 API 엔드포인트

### 인증
- `POST /api/auth/login` - 로그인
- `GET /api/auth/me` - 사용자 정보 조회

### 대시보드
- `GET /api/dashboard` - 대시보드 데이터

### 스케줄
- `GET /api/schedules/:year/:month` - 월별 스케줄 조회
- `POST /api/schedules/:year/:month` - 스케줄 저장

### 요청사항
- `GET /api/requests` - 요청사항 목록
- `POST /api/requests/vacation` - 휴가 신청
- `POST /api/requests/schedule-change` - 스케줄 변경 요청
- `POST /api/requests/room` - 방 배정 요청

### 방 관리
- `GET /api/rooms` - 방 목록 조회
- `PUT /api/rooms/:roomId` - 방 상태 업데이트

### 관리자 전용
- `GET /api/admin/staff` - 직원 목록
- `POST /api/admin/staff` - 직원 추가
- `PUT /api/admin/staff/:staffId` - 직원 수정
- `DELETE /api/admin/staff/:staffId` - 직원 삭제
- `POST /api/admin/auto-assign` - 자동 배정
- `GET /api/admin/reports/:year/:month` - 리포트 생성

## 🚀 배포 가이드

### PM2를 사용한 배포
```bash
# PM2 전역 설치
npm install -g pm2

# 애플리케이션 시작
pm2 start ecosystem.config.js

# 상태 확인
pm2 status

# 로그 확인
pm2 logs gc-endoscopy-system

# 재시작
pm2 restart gc-endoscopy-system

# 중지
pm2 stop gc-endoscopy-system
```

### 로그 디렉토리 생성
```bash
mkdir -p logs
```

## 🔧 환경 설정

### 환경 변수 (.env 파일)
```
NODE_ENV=production
PORT=3000
JWT_SECRET=your-jwt-secret-key
DB_CONNECTION_STRING=your-database-connection
GOOGLE_SHEETS_API_KEY=your-google-api-key
```

## 🤝 기여하기

1. 이 레포지토리를 포크합니다
2. 새 브랜치를 생성합니다 (`git checkout -b feature/새기능`)
3. 변경사항을 커밋합니다 (`git commit -am '새 기능 추가'`)
4. 브랜치에 푸시합니다 (`git push origin feature/새기능`)
5. Pull Request를 생성합니다

## 📝 라이센스

이 프로젝트는 내부 사용을 위한 비공개 소프트웨어입니다.

## 🆘 지원 및 문의

- **개발팀**: development@gchealthcare.co.kr
- **기술지원**: support@gchealthcare.co.kr
- **버그 리포트**: [GitHub Issues](https://github.com/company/gc-endoscopy-room/issues)

## 📈 버전 히스토리

### v1.0.0 (2024-08-27)
- ✨ 초기 웹 애플리케이션 버전 출시
- 🎨 현대적인 반응형 UI/UX 구현
- 🔐 사용자 인증 시스템 구축
- 📅 스케줄 관리 기능 개발
- 🏠 방 배정 시스템 구축
- 👨‍💼 관리자 대시보드 개발
- 📱 모바일 최적화 완료

---

**Made with ❤️ by GC Healthcare Development Team**