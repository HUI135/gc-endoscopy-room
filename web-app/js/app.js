// GC 내시경실 관리시스템 - 메인 JavaScript
class EndoscopyManager {
    constructor() {
        this.currentUser = null;
        this.isAdmin = false;
        this.currentPage = 'home';
        this.mockData = this.initializeMockData();
        
        this.init();
    }

    // 초기화
    init() {
        this.setupEventListeners();
        this.checkAuthStatus();
        this.generateMonthOptions();
        this.showLoading(false);
    }

    // 모의 데이터 초기화
    initializeMockData() {
        return {
            users: [
                { id: '001', name: '김의사', password: 'user123', role: 'doctor', department: '내시경실' },
                { id: '002', name: '이간호사', password: 'user123', role: 'nurse', department: '내시경실' },
                { id: '003', name: '박기사', password: 'user123', role: 'technician', department: '내시경실' },
                { id: 'admin', name: '관리자', password: 'admin123', role: 'admin', department: '관리부' }
            ],
            schedules: {},
            rooms: [
                { id: 'room1', name: '1번 방', status: 'available', assignedTo: null, timeSlot: null },
                { id: 'room2', name: '2번 방', status: 'occupied', assignedTo: '김의사', timeSlot: 'morning' },
                { id: 'room3', name: '3번 방', status: 'available', assignedTo: null, timeSlot: null },
                { id: 'room4', name: '4번 방', status: 'occupied', assignedTo: '이간호사', timeSlot: 'afternoon' }
            ],
            requests: [],
            activities: [
                { type: 'login', message: '김의사님이 로그인했습니다.', time: '10분 전', icon: 'fa-sign-in-alt' },
                { type: 'request', message: '이간호사님이 휴가를 신청했습니다.', time: '30분 전', icon: 'fa-calendar-times' },
                { type: 'assignment', message: '방 배정이 업데이트되었습니다.', time: '1시간 전', icon: 'fa-door-open' },
                { type: 'schedule', message: '스케줄이 변경되었습니다.', time: '2시간 전', icon: 'fa-calendar-alt' }
            ]
        };
    }

    // 이벤트 리스너 설정
    setupEventListeners() {
        // 네비게이션
        document.querySelectorAll('.nav-link').forEach(link => {
            link.addEventListener('click', (e) => this.handleNavigation(e));
        });

        // 로그인 폼
        const loginForm = document.getElementById('login-form');
        if (loginForm) {
            loginForm.addEventListener('submit', (e) => this.handleLogin(e));
        }

        // 로그아웃 버튼
        const logoutBtn = document.getElementById('logout-btn');
        if (logoutBtn) {
            logoutBtn.addEventListener('click', () => this.handleLogout());
        }

        // 탭 버튼들
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.addEventListener('click', (e) => this.handleTabSwitch(e));
        });

        // 각종 폼들
        this.setupFormListeners();

        // 관리자 기능
        this.setupAdminListeners();

        // 모달 닫기
        document.querySelectorAll('.close').forEach(closeBtn => {
            closeBtn.addEventListener('click', (e) => {
                const modal = e.target.closest('.modal');
                if (modal) this.closeModal(modal.id);
            });
        });

        // 모달 외부 클릭 시 닫기
        document.querySelectorAll('.modal').forEach(modal => {
            modal.addEventListener('click', (e) => {
                if (e.target === modal) {
                    this.closeModal(modal.id);
                }
            });
        });
    }

    // 폼 이벤트 리스너 설정
    setupFormListeners() {
        // 휴가 신청 폼
        const vacationForm = document.getElementById('vacation-form');
        if (vacationForm) {
            vacationForm.addEventListener('submit', (e) => this.handleVacationRequest(e));
        }

        // 스케줄 변경 폼
        const scheduleChangeForm = document.getElementById('schedule-change-form');
        if (scheduleChangeForm) {
            scheduleChangeForm.addEventListener('submit', (e) => this.handleScheduleChange(e));
        }

        // 방 배정 요청 폼
        const roomRequestForm = document.getElementById('room-request-form');
        if (roomRequestForm) {
            roomRequestForm.addEventListener('submit', (e) => this.handleRoomRequest(e));
        }

        // 스케줄 관리
        const loadScheduleBtn = document.getElementById('load-schedule');
        if (loadScheduleBtn) {
            loadScheduleBtn.addEventListener('click', () => this.loadSchedule());
        }

        const saveScheduleBtn = document.getElementById('save-schedule');
        if (saveScheduleBtn) {
            saveScheduleBtn.addEventListener('click', () => this.saveSchedule());
        }
    }

    // 관리자 기능 이벤트 리스너
    setupAdminListeners() {
        // 직원 추가
        const addStaffBtn = document.getElementById('add-staff');
        if (addStaffBtn) {
            addStaffBtn.addEventListener('click', () => this.showAddStaffModal());
        }

        // 자동 배정
        const autoAssignBtn = document.getElementById('auto-assign');
        if (autoAssignBtn) {
            autoAssignBtn.addEventListener('click', () => this.autoAssignSchedule());
        }

        // 배정 저장
        const saveAssignmentsBtn = document.getElementById('save-assignments');
        if (saveAssignmentsBtn) {
            saveAssignmentsBtn.addEventListener('click', () => this.saveAssignments());
        }

        // 방 설정 업데이트
        const updateRoomsBtn = document.getElementById('update-rooms');
        if (updateRoomsBtn) {
            updateRoomsBtn.addEventListener('click', () => this.updateRooms());
        }

        // 리포트 생성
        const generateReportBtn = document.getElementById('generate-report');
        if (generateReportBtn) {
            generateReportBtn.addEventListener('click', () => this.generateReport());
        }

        // Excel 다운로드
        const exportExcelBtn = document.getElementById('export-excel');
        if (exportExcelBtn) {
            exportExcelBtn.addEventListener('click', () => this.exportToExcel());
        }
    }

    // 네비게이션 처리
    handleNavigation(e) {
        e.preventDefault();
        const page = e.target.dataset.page;
        if (page) {
            this.showPage(page);
        }
    }

    // 페이지 표시
    showPage(pageId) {
        // 모든 페이지 숨기기
        document.querySelectorAll('.page').forEach(page => {
            page.classList.remove('active');
        });

        // 모든 네비게이션 링크 비활성화
        document.querySelectorAll('.nav-link').forEach(link => {
            link.classList.remove('active');
        });

        // 선택된 페이지 표시
        const targetPage = document.getElementById(`page-${pageId}`);
        const targetNav = document.querySelector(`[data-page="${pageId}"]`);
        
        if (targetPage) {
            targetPage.classList.add('active');
            this.currentPage = pageId;
        }
        
        if (targetNav) {
            targetNav.classList.add('active');
        }

        // 페이지별 초기화
        this.initializePage(pageId);
    }

    // 페이지별 초기화
    initializePage(pageId) {
        switch (pageId) {
            case 'home':
                this.updateDashboard();
                break;
            case 'schedule':
                this.renderSchedule();
                break;
            case 'request':
                this.populateStaffDropdown();
                break;
            case 'room':
                this.renderRoomLayout();
                break;
            case 'admin':
                if (this.isAdmin) {
                    this.renderStaffTable();
                    this.renderAdminDashboard();
                }
                break;
        }
    }

    // 로그인 처리
    async handleLogin(e) {
        e.preventDefault();
        this.showLoading(true);

        const formData = new FormData(e.target);
        const employeeId = formData.get('employee-id');
        const password = formData.get('password');

        // 모의 인증
        await this.delay(1000); // 로딩 시뮬레이션

        const user = this.mockData.users.find(u => u.id === employeeId && u.password === password);
        
        if (user) {
            this.currentUser = user;
            this.isAdmin = user.role === 'admin';
            
            // UI 업데이트
            this.updateAuthUI();
            this.showNotification('로그인 성공', `${user.name}님, 환영합니다!`, 'success');
            
            // 활동 기록 추가
            this.addActivity('login', `${user.name}님이 로그인했습니다.`);
        } else {
            this.showNotification('로그인 실패', '사번 또는 비밀번호가 올바르지 않습니다.', 'error');
        }

        this.showLoading(false);
    }

    // 로그아웃 처리
    handleLogout() {
        this.showConfirm('로그아웃', '정말 로그아웃하시겠습니까?', () => {
            this.currentUser = null;
            this.isAdmin = false;
            this.updateAuthUI();
            this.showPage('home');
            this.showNotification('로그아웃', '성공적으로 로그아웃되었습니다.', 'info');
        });
    }

    // 인증 상태 확인
    checkAuthStatus() {
        // 실제 구현에서는 세션/토큰 확인
        const storedUser = localStorage.getItem('currentUser');
        if (storedUser) {
            this.currentUser = JSON.parse(storedUser);
            this.isAdmin = this.currentUser.role === 'admin';
            this.updateAuthUI();
        }
    }

    // 인증 UI 업데이트
    updateAuthUI() {
        const loginSection = document.getElementById('login-section');
        const dashboardSection = document.getElementById('dashboard-section');
        const userNameElement = document.getElementById('user-name');
        const logoutBtn = document.getElementById('logout-btn');

        if (this.currentUser) {
            // 로그인 상태
            loginSection.style.display = 'none';
            dashboardSection.style.display = 'block';
            userNameElement.textContent = this.currentUser.name;
            logoutBtn.style.display = 'block';

            // 관리자 메뉴 표시/숨김
            if (this.isAdmin) {
                document.body.classList.add('admin');
            } else {
                document.body.classList.remove('admin');
            }

            // 세션 저장
            localStorage.setItem('currentUser', JSON.stringify(this.currentUser));
        } else {
            // 로그아웃 상태
            loginSection.style.display = 'flex';
            dashboardSection.style.display = 'none';
            userNameElement.textContent = '로그인하세요';
            logoutBtn.style.display = 'none';
            document.body.classList.remove('admin');

            // 세션 제거
            localStorage.removeItem('currentUser');
        }
    }

    // 대시보드 업데이트
    updateDashboard() {
        if (!this.currentUser) return;

        // 통계 업데이트
        document.getElementById('my-shifts').textContent = this.getMyShifts();
        document.getElementById('assigned-rooms').textContent = this.getAssignedRooms();
        document.getElementById('pending-requests').textContent = this.getPendingRequests();
        document.getElementById('total-staff').textContent = this.mockData.users.length;

        // 환영 메시지
        const welcomeMsg = document.getElementById('welcome-message');
        const currentHour = new Date().getHours();
        let greeting = '안녕하세요';
        if (currentHour < 12) greeting = '좋은 아침입니다';
        else if (currentHour < 18) greeting = '좋은 오후입니다';
        else greeting = '좋은 저녁입니다';
        
        welcomeMsg.textContent = `${greeting}, ${this.currentUser.name}님!`;

        // 최근 활동 렌더링
        this.renderRecentActivities();
    }

    // 내 근무일 수 계산
    getMyShifts() {
        // 모의 데이터: 현재 월 근무일
        return Math.floor(Math.random() * 15) + 10;
    }

    // 배정된 방 수 계산
    getAssignedRooms() {
        return this.mockData.rooms.filter(room => 
            room.assignedTo === this.currentUser?.name
        ).length;
    }

    // 대기중인 요청 수 계산
    getPendingRequests() {
        return this.mockData.requests.filter(req => 
            req.userId === this.currentUser?.id && req.status === 'pending'
        ).length;
    }

    // 최근 활동 렌더링
    renderRecentActivities() {
        const activityList = document.getElementById('activity-list');
        if (!activityList) return;

        activityList.innerHTML = '';
        
        this.mockData.activities.forEach(activity => {
            const activityItem = document.createElement('div');
            activityItem.className = 'activity-item';
            activityItem.innerHTML = `
                <div class="activity-icon">
                    <i class="fas ${activity.icon}"></i>
                </div>
                <div class="activity-content">
                    <div>${activity.message}</div>
                    <div class="activity-time">${activity.time}</div>
                </div>
            `;
            activityList.appendChild(activityItem);
        });
    }

    // 활동 추가
    addActivity(type, message) {
        const iconMap = {
            login: 'fa-sign-in-alt',
            logout: 'fa-sign-out-alt',
            request: 'fa-paper-plane',
            schedule: 'fa-calendar-alt',
            room: 'fa-door-open'
        };

        this.mockData.activities.unshift({
            type,
            message,
            time: '방금 전',
            icon: iconMap[type] || 'fa-info-circle'
        });

        // 최대 10개까지만 유지
        if (this.mockData.activities.length > 10) {
            this.mockData.activities = this.mockData.activities.slice(0, 10);
        }

        this.renderRecentActivities();
    }

    // 탭 전환 처리
    handleTabSwitch(e) {
        const tabName = e.target.dataset.tab;
        const parentContainer = e.target.closest('.request-tabs, .admin-tabs').parentElement;
        
        // 모든 탭 버튼 비활성화
        parentContainer.querySelectorAll('.tab-btn').forEach(btn => {
            btn.classList.remove('active');
        });
        
        // 모든 탭 패널 숨기기
        parentContainer.querySelectorAll('.tab-pane').forEach(pane => {
            pane.classList.remove('active');
        });
        
        // 선택된 탭 활성화
        e.target.classList.add('active');
        const targetPane = document.getElementById(`${tabName}-tab`);
        if (targetPane) {
            targetPane.classList.add('active');
        }
    }

    // 휴가 신청 처리
    async handleVacationRequest(e) {
        e.preventDefault();
        this.showLoading(true);

        const formData = new FormData(e.target);
        const request = {
            id: Date.now().toString(),
            userId: this.currentUser?.id,
            type: 'vacation',
            date: formData.get('vacation-date'),
            vacationType: formData.get('vacation-type'),
            reason: formData.get('vacation-reason'),
            status: 'pending',
            createdAt: new Date().toISOString()
        };

        // 모의 API 호출
        await this.delay(1000);

        this.mockData.requests.push(request);
        this.addActivity('request', '휴가 신청이 제출되었습니다.');
        
        this.showNotification('휴가 신청', '휴가 신청이 성공적으로 제출되었습니다.', 'success');
        e.target.reset();
        
        this.showLoading(false);
    }

    // 스케줄 변경 처리
    async handleScheduleChange(e) {
        e.preventDefault();
        this.showLoading(true);

        const formData = new FormData(e.target);
        const request = {
            id: Date.now().toString(),
            userId: this.currentUser?.id,
            type: 'schedule_change',
            exchangeWith: formData.get('exchange-with'),
            myDate: formData.get('my-date'),
            theirDate: formData.get('their-date'),
            reason: formData.get('exchange-reason'),
            status: 'pending',
            createdAt: new Date().toISOString()
        };

        await this.delay(1000);

        this.mockData.requests.push(request);
        this.addActivity('request', '스케줄 교환 요청이 제출되었습니다.');
        
        this.showNotification('스케줄 교환', '스케줄 교환 요청이 성공적으로 제출되었습니다.', 'success');
        e.target.reset();
        
        this.showLoading(false);
    }

    // 방 배정 요청 처리
    async handleRoomRequest(e) {
        e.preventDefault();
        this.showLoading(true);

        const formData = new FormData(e.target);
        const request = {
            id: Date.now().toString(),
            userId: this.currentUser?.id,
            type: 'room_request',
            preferredRoom: formData.get('preferred-room'),
            preferredTime: formData.get('preferred-time'),
            reason: formData.get('room-request-reason'),
            status: 'pending',
            createdAt: new Date().toISOString()
        };

        await this.delay(1000);

        this.mockData.requests.push(request);
        this.addActivity('request', '방 배정 요청이 제출되었습니다.');
        
        this.showNotification('방 배정 요청', '방 배정 요청이 성공적으로 제출되었습니다.', 'success');
        e.target.reset();
        
        this.showLoading(false);
    }

    // 직원 드롭다운 채우기
    populateStaffDropdown() {
        const dropdown = document.getElementById('exchange-with');
        if (!dropdown) return;

        dropdown.innerHTML = '<option value="">직원 선택</option>';
        
        this.mockData.users
            .filter(user => user.role !== 'admin' && user.id !== this.currentUser?.id)
            .forEach(user => {
                const option = document.createElement('option');
                option.value = user.id;
                option.textContent = user.name;
                dropdown.appendChild(option);
            });
    }

    // 스케줄 렌더링
    renderSchedule() {
        const calendarContainer = document.getElementById('calendar-container');
        if (!calendarContainer) return;

        // 간단한 캘린더 생성 (실제로는 더 복잡한 캘린더 라이브러리 사용)
        calendarContainer.innerHTML = `
            <div class="calendar-placeholder">
                <h3>📅 ${new Date().getFullYear()}년 ${new Date().getMonth() + 1}월 스케줄</h3>
                <p>스케줄 캘린더가 여기에 표시됩니다.</p>
                <div class="schedule-grid">
                    ${this.generateCalendarDays()}
                </div>
            </div>
        `;
    }

    // 캘린더 일수 생성
    generateCalendarDays() {
        const today = new Date();
        const year = today.getFullYear();
        const month = today.getMonth();
        const daysInMonth = new Date(year, month + 1, 0).getDate();
        
        let html = '<div class="calendar-grid">';
        
        // 요일 헤더
        const weekdays = ['일', '월', '화', '수', '목', '금', '토'];
        weekdays.forEach(day => {
            html += `<div class="calendar-header">${day}</div>`;
        });
        
        // 월 시작일의 요일 계산
        const firstDay = new Date(year, month, 1).getDay();
        
        // 빈 날짜 채우기
        for (let i = 0; i < firstDay; i++) {
            html += '<div class="calendar-day empty"></div>';
        }
        
        // 실제 날짜들
        for (let day = 1; day <= daysInMonth; day++) {
            const isToday = day === today.getDate();
            const hasSchedule = Math.random() > 0.7; // 30% 확률로 스케줄 있음
            
            html += `
                <div class="calendar-day ${isToday ? 'today' : ''} ${hasSchedule ? 'has-schedule' : ''}">
                    <span class="day-number">${day}</span>
                    ${hasSchedule ? '<div class="schedule-indicator"></div>' : ''}
                </div>
            `;
        }
        
        html += '</div>';
        
        // CSS 스타일 추가
        const style = `
            <style>
            .calendar-grid {
                display: grid;
                grid-template-columns: repeat(7, 1fr);
                gap: 1px;
                background: #ddd;
                margin-top: 1rem;
            }
            .calendar-header {
                background: #f8f9fa;
                padding: 0.75rem;
                text-align: center;
                font-weight: 600;
                color: #495057;
            }
            .calendar-day {
                background: white;
                min-height: 60px;
                padding: 0.5rem;
                position: relative;
                cursor: pointer;
                transition: background-color 0.2s;
            }
            .calendar-day:hover {
                background: #f8f9fa;
            }
            .calendar-day.today {
                background: #e3f2fd;
                font-weight: 600;
            }
            .calendar-day.has-schedule {
                background: #fff3e0;
            }
            .calendar-day.empty {
                background: #f8f9fa;
                cursor: default;
            }
            .day-number {
                font-size: 0.9rem;
                color: #495057;
            }
            .schedule-indicator {
                position: absolute;
                bottom: 4px;
                right: 4px;
                width: 8px;
                height: 8px;
                background: #ff9800;
                border-radius: 50%;
            }
            .calendar-placeholder h3 {
                text-align: center;
                color: #495057;
                margin-bottom: 1rem;
            }
            </style>
        `;
        
        return style + html;
    }

    // 방 레이아웃 렌더링
    renderRoomLayout() {
        const roomGrid = document.getElementById('room-grid');
        if (!roomGrid) return;

        roomGrid.innerHTML = '';
        
        this.mockData.rooms.forEach(room => {
            const roomCard = document.createElement('div');
            roomCard.className = `room-card ${room.status}`;
            roomCard.innerHTML = `
                <div class="room-number">${room.name}</div>
                <div class="room-status">${this.getRoomStatusText(room.status)}</div>
                ${room.assignedTo ? `<div class="room-occupant">${room.assignedTo}</div>` : ''}
                ${room.timeSlot ? `<div class="room-time">${this.getTimeSlotText(room.timeSlot)}</div>` : ''}
            `;
            roomGrid.appendChild(roomCard);
        });
    }

    // 방 상태 텍스트
    getRoomStatusText(status) {
        const statusMap = {
            available: '사용 가능',
            occupied: '사용 중',
            maintenance: '정비 중'
        };
        return statusMap[status] || status;
    }

    // 시간대 텍스트
    getTimeSlotText(timeSlot) {
        const timeMap = {
            morning: '오전 (09:00-12:00)',
            afternoon: '오후 (13:00-17:00)'
        };
        return timeMap[timeSlot] || timeSlot;
    }

    // 직원 테이블 렌더링
    renderStaffTable() {
        const container = document.getElementById('staff-table-container');
        if (!container) return;

        const table = document.createElement('table');
        table.className = 'data-table';
        table.innerHTML = `
            <thead>
                <tr>
                    <th>사번</th>
                    <th>이름</th>
                    <th>직책</th>
                    <th>부서</th>
                    <th>상태</th>
                    <th>작업</th>
                </tr>
            </thead>
            <tbody>
                ${this.mockData.users.map(user => `
                    <tr>
                        <td>${user.id}</td>
                        <td>${user.name}</td>
                        <td>${this.getRoleText(user.role)}</td>
                        <td>${user.department}</td>
                        <td><span class="status-badge active">활성</span></td>
                        <td>
                            <button class="btn btn-sm btn-outline" onclick="endoscopyManager.editStaff('${user.id}')">
                                <i class="fas fa-edit"></i>
                            </button>
                            <button class="btn btn-sm btn-danger" onclick="endoscopyManager.deleteStaff('${user.id}')">
                                <i class="fas fa-trash"></i>
                            </button>
                        </td>
                    </tr>
                `).join('')}
            </tbody>
        `;
        
        container.innerHTML = '';
        container.appendChild(table);

        // 테이블 스타일 추가
        const style = document.createElement('style');
        style.textContent = `
            .btn-sm {
                padding: 0.25rem 0.5rem;
                font-size: 0.875rem;
                margin: 0 0.125rem;
            }
            .status-badge {
                padding: 0.25rem 0.5rem;
                border-radius: 0.25rem;
                font-size: 0.75rem;
                font-weight: 500;
            }
            .status-badge.active {
                background: #d1e7dd;
                color: #0f5132;
            }
        `;
        document.head.appendChild(style);
    }

    // 직책 텍스트
    getRoleText(role) {
        const roleMap = {
            doctor: '의사',
            nurse: '간호사',
            technician: '기사',
            admin: '관리자'
        };
        return roleMap[role] || role;
    }

    // 관리자 대시보드 렌더링
    renderAdminDashboard() {
        // 관리자용 추가 기능들을 여기에 구현
        console.log('관리자 대시보드 렌더링');
    }

    // 월 옵션 생성
    generateMonthOptions() {
        const selectors = document.querySelectorAll('#month-selector, #report-month');
        const currentDate = new Date();
        
        selectors.forEach(selector => {
            if (!selector) return;
            
            selector.innerHTML = '<option value="">월 선택</option>';
            
            for (let i = 0; i < 12; i++) {
                const date = new Date(currentDate.getFullYear(), i, 1);
                const option = document.createElement('option');
                option.value = i + 1;
                option.textContent = `${date.getFullYear()}년 ${i + 1}월`;
                if (i === currentDate.getMonth()) {
                    option.selected = true;
                }
                selector.appendChild(option);
            }
        });
    }

    // 스케줄 로드
    async loadSchedule() {
        const monthSelector = document.getElementById('month-selector');
        const selectedMonth = monthSelector?.value;
        
        if (!selectedMonth) {
            this.showNotification('월 선택 필요', '먼저 월을 선택해주세요.', 'warning');
            return;
        }

        this.showLoading(true);
        await this.delay(1000);
        
        this.renderSchedule();
        this.showNotification('스케줄 로드', `${selectedMonth}월 스케줄을 불러왔습니다.`, 'success');
        this.showLoading(false);
    }

    // 스케줄 저장
    async saveSchedule() {
        this.showConfirm('스케줄 저장', '현재 스케줄을 저장하시겠습니까?', async () => {
            this.showLoading(true);
            await this.delay(1500);
            
            this.addActivity('schedule', '스케줄이 저장되었습니다.');
            this.showNotification('스케줄 저장', '스케줄이 성공적으로 저장되었습니다.', 'success');
            this.showLoading(false);
        });
    }

    // 자동 스케줄 배정
    async autoAssignSchedule() {
        this.showConfirm('자동 배정', '자동으로 스케줄을 배정하시겠습니까?', async () => {
            this.showLoading(true);
            await this.delay(2000);
            
            // 모의 배정 결과
            const result = document.getElementById('assignment-result');
            if (result) {
                result.innerHTML = `
                    <div class="assignment-success">
                        <h4><i class="fas fa-check-circle"></i> 자동 배정 완료</h4>
                        <p>총 ${this.mockData.users.length - 1}명의 직원에 대해 스케줄이 배정되었습니다.</p>
                        <ul>
                            <li>김의사: 15일 배정</li>
                            <li>이간호사: 14일 배정</li>
                            <li>박기사: 13일 배정</li>
                        </ul>
                    </div>
                `;
            }
            
            this.addActivity('schedule', '자동 스케줄 배정이 완료되었습니다.');
            this.showNotification('자동 배정', '스케줄 자동 배정이 완료되었습니다.', 'success');
            this.showLoading(false);
        });
    }

    // 배정 저장
    async saveAssignments() {
        this.showConfirm('배정 저장', '현재 배정 결과를 저장하시겠습니까?', async () => {
            this.showLoading(true);
            await this.delay(1000);
            
            this.addActivity('schedule', '배정 결과가 저장되었습니다.');
            this.showNotification('배정 저장', '배정 결과가 성공적으로 저장되었습니다.', 'success');
            this.showLoading(false);
        });
    }

    // 방 설정 업데이트
    async updateRooms() {
        const roomCount = document.getElementById('room-count')?.value;
        if (!roomCount) return;

        this.showLoading(true);
        await this.delay(1000);

        // 방 개수에 따라 mock data 업데이트
        this.mockData.rooms = [];
        for (let i = 1; i <= parseInt(roomCount); i++) {
            this.mockData.rooms.push({
                id: `room${i}`,
                name: `${i}번 방`,
                status: 'available',
                assignedTo: null,
                timeSlot: null
            });
        }

        this.renderRoomLayout();
        this.addActivity('room', `방 설정이 ${roomCount}개로 업데이트되었습니다.`);
        this.showNotification('방 설정', `방 개수가 ${roomCount}개로 업데이트되었습니다.`, 'success');
        this.showLoading(false);
    }

    // 리포트 생성
    async generateReport() {
        const reportMonth = document.getElementById('report-month')?.value;
        if (!reportMonth) {
            this.showNotification('월 선택 필요', '먼저 월을 선택해주세요.', 'warning');
            return;
        }

        this.showLoading(true);
        await this.delay(1500);

        const reportContent = document.getElementById('report-content');
        if (reportContent) {
            reportContent.innerHTML = `
                <div class="report-summary">
                    <h4><i class="fas fa-chart-bar"></i> ${reportMonth}월 근무 리포트</h4>
                    <div class="report-stats">
                        <div class="report-stat">
                            <label>총 근무일:</label>
                            <span>22일</span>
                        </div>
                        <div class="report-stat">
                            <label>총 근무자:</label>
                            <span>${this.mockData.users.length - 1}명</span>
                        </div>
                        <div class="report-stat">
                            <label>휴가 신청:</label>
                            <span>8건</span>
                        </div>
                        <div class="report-stat">
                            <label>방 가동률:</label>
                            <span>85%</span>
                        </div>
                    </div>
                </div>
            `;
        }

        this.showNotification('리포트 생성', `${reportMonth}월 리포트가 생성되었습니다.`, 'success');
        this.showLoading(false);
    }

    // Excel 다운로드
    async exportToExcel() {
        this.showLoading(true);
        await this.delay(1000);

        // 실제로는 서버에서 Excel 파일을 생성하고 다운로드
        this.showNotification('Excel 다운로드', 'Excel 파일이 다운로드됩니다.', 'success');
        this.showLoading(false);
    }

    // 직원 편집
    editStaff(userId) {
        this.showNotification('편집', `${userId} 직원 정보를 편집합니다.`, 'info');
    }

    // 직원 삭제
    deleteStaff(userId) {
        this.showConfirm('직원 삭제', '정말 이 직원을 삭제하시겠습니까?', () => {
            this.mockData.users = this.mockData.users.filter(user => user.id !== userId);
            this.renderStaffTable();
            this.showNotification('직원 삭제', '직원이 삭제되었습니다.', 'success');
        });
    }

    // 유틸리티 함수들
    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    showLoading(show) {
        const overlay = document.getElementById('loading-overlay');
        if (overlay) {
            if (show) {
                overlay.classList.add('show');
            } else {
                overlay.classList.remove('show');
            }
        }
    }

    showNotification(title, message, type = 'info') {
        const modal = document.getElementById('notification-modal');
        const titleEl = document.getElementById('notification-title');
        const messageEl = document.getElementById('notification-message');
        const iconEl = document.getElementById('notification-icon');

        if (modal && titleEl && messageEl && iconEl) {
            titleEl.textContent = title;
            messageEl.textContent = message;
            
            // 아이콘 및 색상 설정
            const iconMap = {
                success: 'fa-check-circle',
                error: 'fa-exclamation-circle',
                warning: 'fa-exclamation-triangle',
                info: 'fa-info-circle'
            };
            
            iconEl.className = `fas ${iconMap[type] || iconMap.info}`;
            modal.classList.add('show');
        }
    }

    showConfirm(title, message, onConfirm) {
        const modal = document.getElementById('confirm-modal');
        const titleEl = document.getElementById('confirm-title');
        const messageEl = document.getElementById('confirm-message');
        const confirmBtn = document.getElementById('confirm-yes');

        if (modal && titleEl && messageEl && confirmBtn) {
            titleEl.textContent = title;
            messageEl.textContent = message;
            
            // 기존 이벤트 리스너 제거
            confirmBtn.replaceWith(confirmBtn.cloneNode(true));
            const newConfirmBtn = document.getElementById('confirm-yes');
            
            newConfirmBtn.addEventListener('click', () => {
                this.closeModal('confirm-modal');
                if (onConfirm) onConfirm();
            });
            
            modal.classList.add('show');
        }
    }

    closeModal(modalId) {
        const modal = document.getElementById(modalId);
        if (modal) {
            modal.classList.remove('show');
        }
    }
}

// 전역 변수로 인스턴스 생성
let endoscopyManager;

// DOM 로드 완료 시 초기화
document.addEventListener('DOMContentLoaded', () => {
    endoscopyManager = new EndoscopyManager();
});

// 전역 함수들 (HTML에서 직접 호출하는 경우)
function closeModal(modalId) {
    if (endoscopyManager) {
        endoscopyManager.closeModal(modalId);
    }
}