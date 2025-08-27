const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;

// 미들웨어 설정
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '..')));

// 모의 데이터베이스 (실제로는 Google Sheets API 또는 실제 DB 사용)
let mockDatabase = {
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

// 유틸리티 함수들
const generateId = () => Date.now().toString();
const getCurrentTime = () => new Date().toISOString();

// 인증 미들웨어
const authenticateUser = (req, res, next) => {
    const token = req.headers.authorization;
    
    // 실제로는 JWT 토큰 검증
    if (!token) {
        return res.status(401).json({ error: '인증 토큰이 필요합니다.' });
    }
    
    // 간단한 토큰 검증 (실제로는 더 복잡한 로직)
    const userId = token.replace('Bearer ', '');
    const user = mockDatabase.users.find(u => u.id === userId);
    
    if (!user) {
        return res.status(401).json({ error: '유효하지 않은 토큰입니다.' });
    }
    
    req.user = user;
    next();
};

// API 라우트들

// 메인 페이지 제공
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, '..', 'index.html'));
});

// 로그인
app.post('/api/auth/login', (req, res) => {
    const { employeeId, password } = req.body;
    
    const user = mockDatabase.users.find(u => u.id === employeeId && u.password === password);
    
    if (user) {
        // 실제로는 JWT 토큰 생성
        const token = user.id; // 간단한 토큰
        
        // 활동 기록 추가
        mockDatabase.activities.unshift({
            type: 'login',
            message: `${user.name}님이 로그인했습니다.`,
            time: '방금 전',
            icon: 'fa-sign-in-alt',
            timestamp: getCurrentTime()
        });
        
        res.json({
            success: true,
            token,
            user: {
                id: user.id,
                name: user.name,
                role: user.role,
                department: user.department
            }
        });
    } else {
        res.status(401).json({
            success: false,
            error: '사번 또는 비밀번호가 올바르지 않습니다.'
        });
    }
});

// 사용자 정보 조회
app.get('/api/auth/me', authenticateUser, (req, res) => {
    res.json({
        success: true,
        user: req.user
    });
});

// 대시보드 데이터 조회
app.get('/api/dashboard', authenticateUser, (req, res) => {
    const myShifts = Math.floor(Math.random() * 15) + 10;
    const assignedRooms = mockDatabase.rooms.filter(room => room.assignedTo === req.user.name).length;
    const pendingRequests = mockDatabase.requests.filter(req => req.userId === req.user.id && req.status === 'pending').length;
    
    res.json({
        success: true,
        data: {
            myShifts,
            assignedRooms,
            pendingRequests,
            totalStaff: mockDatabase.users.length,
            activities: mockDatabase.activities.slice(0, 10)
        }
    });
});

// 스케줄 관련 API

// 월별 스케줄 조회
app.get('/api/schedules/:year/:month', authenticateUser, (req, res) => {
    const { year, month } = req.params;
    const key = `${year}-${month}`;
    
    const schedule = mockDatabase.schedules[key] || {
        year: parseInt(year),
        month: parseInt(month),
        days: generateMonthSchedule(year, month, req.user.id)
    };
    
    mockDatabase.schedules[key] = schedule;
    
    res.json({
        success: true,
        schedule
    });
});

// 스케줄 저장
app.post('/api/schedules/:year/:month', authenticateUser, (req, res) => {
    const { year, month } = req.params;
    const { days } = req.body;
    const key = `${year}-${month}`;
    
    mockDatabase.schedules[key] = {
        year: parseInt(year),
        month: parseInt(month),
        days,
        updatedBy: req.user.id,
        updatedAt: getCurrentTime()
    };
    
    // 활동 기록 추가
    mockDatabase.activities.unshift({
        type: 'schedule',
        message: `${req.user.name}님이 ${month}월 스케줄을 수정했습니다.`,
        time: '방금 전',
        icon: 'fa-calendar-alt',
        timestamp: getCurrentTime()
    });
    
    res.json({
        success: true,
        message: '스케줄이 저장되었습니다.'
    });
});

// 요청사항 관련 API

// 요청사항 목록 조회
app.get('/api/requests', authenticateUser, (req, res) => {
    let requests = mockDatabase.requests;
    
    // 관리자가 아닌 경우 본인 요청만 조회
    if (req.user.role !== 'admin') {
        requests = requests.filter(request => request.userId === req.user.id);
    }
    
    res.json({
        success: true,
        requests: requests.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt))
    });
});

// 휴가 신청
app.post('/api/requests/vacation', authenticateUser, (req, res) => {
    const { date, vacationType, reason } = req.body;
    
    const request = {
        id: generateId(),
        userId: req.user.id,
        userName: req.user.name,
        type: 'vacation',
        date,
        vacationType,
        reason,
        status: 'pending',
        createdAt: getCurrentTime()
    };
    
    mockDatabase.requests.push(request);
    
    // 활동 기록 추가
    mockDatabase.activities.unshift({
        type: 'request',
        message: `${req.user.name}님이 휴가를 신청했습니다.`,
        time: '방금 전',
        icon: 'fa-calendar-times',
        timestamp: getCurrentTime()
    });
    
    res.json({
        success: true,
        message: '휴가 신청이 제출되었습니다.',
        request
    });
});

// 스케줄 변경 요청
app.post('/api/requests/schedule-change', authenticateUser, (req, res) => {
    const { exchangeWith, myDate, theirDate, reason } = req.body;
    
    const targetUser = mockDatabase.users.find(u => u.id === exchangeWith);
    
    if (!targetUser) {
        return res.status(400).json({
            success: false,
            error: '교환 대상을 찾을 수 없습니다.'
        });
    }
    
    const request = {
        id: generateId(),
        userId: req.user.id,
        userName: req.user.name,
        type: 'schedule_change',
        exchangeWith,
        exchangeWithName: targetUser.name,
        myDate,
        theirDate,
        reason,
        status: 'pending',
        createdAt: getCurrentTime()
    };
    
    mockDatabase.requests.push(request);
    
    // 활동 기록 추가
    mockDatabase.activities.unshift({
        type: 'request',
        message: `${req.user.name}님이 스케줄 교환을 요청했습니다.`,
        time: '방금 전',
        icon: 'fa-exchange-alt',
        timestamp: getCurrentTime()
    });
    
    res.json({
        success: true,
        message: '스케줄 교환 요청이 제출되었습니다.',
        request
    });
});

// 방 배정 요청
app.post('/api/requests/room', authenticateUser, (req, res) => {
    const { preferredRoom, preferredTime, reason } = req.body;
    
    const request = {
        id: generateId(),
        userId: req.user.id,
        userName: req.user.name,
        type: 'room_request',
        preferredRoom,
        preferredTime,
        reason,
        status: 'pending',
        createdAt: getCurrentTime()
    };
    
    mockDatabase.requests.push(request);
    
    // 활동 기록 추가
    mockDatabase.activities.unshift({
        type: 'request',
        message: `${req.user.name}님이 방 배정을 요청했습니다.`,
        time: '방금 전',
        icon: 'fa-door-open',
        timestamp: getCurrentTime()
    });
    
    res.json({
        success: true,
        message: '방 배정 요청이 제출되었습니다.',
        request
    });
});

// 방 관련 API

// 방 목록 조회
app.get('/api/rooms', authenticateUser, (req, res) => {
    res.json({
        success: true,
        rooms: mockDatabase.rooms
    });
});

// 방 상태 업데이트 (관리자만)
app.put('/api/rooms/:roomId', authenticateUser, (req, res) => {
    if (req.user.role !== 'admin') {
        return res.status(403).json({
            success: false,
            error: '관리자 권한이 필요합니다.'
        });
    }
    
    const { roomId } = req.params;
    const { status, assignedTo, timeSlot } = req.body;
    
    const roomIndex = mockDatabase.rooms.findIndex(r => r.id === roomId);
    
    if (roomIndex === -1) {
        return res.status(404).json({
            success: false,
            error: '방을 찾을 수 없습니다.'
        });
    }
    
    mockDatabase.rooms[roomIndex] = {
        ...mockDatabase.rooms[roomIndex],
        status,
        assignedTo,
        timeSlot,
        updatedAt: getCurrentTime()
    };
    
    // 활동 기록 추가
    mockDatabase.activities.unshift({
        type: 'room',
        message: `${roomId} 방 배정이 업데이트되었습니다.`,
        time: '방금 전',
        icon: 'fa-door-open',
        timestamp: getCurrentTime()
    });
    
    res.json({
        success: true,
        message: '방 상태가 업데이트되었습니다.',
        room: mockDatabase.rooms[roomIndex]
    });
});

// 관리자 전용 API

// 전체 직원 목록 조회 (관리자만)
app.get('/api/admin/staff', authenticateUser, (req, res) => {
    if (req.user.role !== 'admin') {
        return res.status(403).json({
            success: false,
            error: '관리자 권한이 필요합니다.'
        });
    }
    
    const staff = mockDatabase.users.map(user => ({
        id: user.id,
        name: user.name,
        role: user.role,
        department: user.department
    }));
    
    res.json({
        success: true,
        staff
    });
});

// 직원 추가 (관리자만)
app.post('/api/admin/staff', authenticateUser, (req, res) => {
    if (req.user.role !== 'admin') {
        return res.status(403).json({
            success: false,
            error: '관리자 권한이 필요합니다.'
        });
    }
    
    const { id, name, password, role, department } = req.body;
    
    // 중복 사번 확인
    if (mockDatabase.users.find(u => u.id === id)) {
        return res.status(400).json({
            success: false,
            error: '이미 존재하는 사번입니다.'
        });
    }
    
    const newUser = { id, name, password, role, department };
    mockDatabase.users.push(newUser);
    
    // 활동 기록 추가
    mockDatabase.activities.unshift({
        type: 'staff',
        message: `새 직원 ${name}이 추가되었습니다.`,
        time: '방금 전',
        icon: 'fa-user-plus',
        timestamp: getCurrentTime()
    });
    
    res.json({
        success: true,
        message: '직원이 추가되었습니다.',
        staff: { id, name, role, department }
    });
});

// 직원 수정 (관리자만)
app.put('/api/admin/staff/:staffId', authenticateUser, (req, res) => {
    if (req.user.role !== 'admin') {
        return res.status(403).json({
            success: false,
            error: '관리자 권한이 필요합니다.'
        });
    }
    
    const { staffId } = req.params;
    const { name, role, department } = req.body;
    
    const staffIndex = mockDatabase.users.findIndex(u => u.id === staffId);
    
    if (staffIndex === -1) {
        return res.status(404).json({
            success: false,
            error: '직원을 찾을 수 없습니다.'
        });
    }
    
    mockDatabase.users[staffIndex] = {
        ...mockDatabase.users[staffIndex],
        name,
        role,
        department
    };
    
    res.json({
        success: true,
        message: '직원 정보가 수정되었습니다.',
        staff: mockDatabase.users[staffIndex]
    });
});

// 직원 삭제 (관리자만)
app.delete('/api/admin/staff/:staffId', authenticateUser, (req, res) => {
    if (req.user.role !== 'admin') {
        return res.status(403).json({
            success: false,
            error: '관리자 권한이 필요합니다.'
        });
    }
    
    const { staffId } = req.params;
    const staffIndex = mockDatabase.users.findIndex(u => u.id === staffId);
    
    if (staffIndex === -1) {
        return res.status(404).json({
            success: false,
            error: '직원을 찾을 수 없습니다.'
        });
    }
    
    const deletedStaff = mockDatabase.users.splice(staffIndex, 1)[0];
    
    // 활동 기록 추가
    mockDatabase.activities.unshift({
        type: 'staff',
        message: `직원 ${deletedStaff.name}이 삭제되었습니다.`,
        time: '방금 전',
        icon: 'fa-user-minus',
        timestamp: getCurrentTime()
    });
    
    res.json({
        success: true,
        message: '직원이 삭제되었습니다.'
    });
});

// 자동 스케줄 배정 (관리자만)
app.post('/api/admin/auto-assign', authenticateUser, (req, res) => {
    if (req.user.role !== 'admin') {
        return res.status(403).json({
            success: false,
            error: '관리자 권한이 필요합니다.'
        });
    }
    
    // 모의 자동 배정 로직
    const assignments = mockDatabase.users
        .filter(user => user.role !== 'admin')
        .map(user => ({
            userId: user.id,
            name: user.name,
            assignedDays: Math.floor(Math.random() * 5) + 13 // 13-17일
        }));
    
    // 활동 기록 추가
    mockDatabase.activities.unshift({
        type: 'schedule',
        message: '자동 스케줄 배정이 완료되었습니다.',
        time: '방금 전',
        icon: 'fa-magic',
        timestamp: getCurrentTime()
    });
    
    res.json({
        success: true,
        message: '자동 배정이 완료되었습니다.',
        assignments
    });
});

// 리포트 생성 (관리자만)
app.get('/api/admin/reports/:year/:month', authenticateUser, (req, res) => {
    if (req.user.role !== 'admin') {
        return res.status(403).json({
            success: false,
            error: '관리자 권한이 필요합니다.'
        });
    }
    
    const { year, month } = req.params;
    
    // 모의 리포트 데이터
    const report = {
        year: parseInt(year),
        month: parseInt(month),
        totalWorkDays: 22,
        totalStaff: mockDatabase.users.length - 1,
        vacationRequests: mockDatabase.requests.filter(r => r.type === 'vacation').length,
        roomUtilization: 85,
        staffWorkDays: mockDatabase.users
            .filter(user => user.role !== 'admin')
            .map(user => ({
                name: user.name,
                workDays: Math.floor(Math.random() * 5) + 13
            }))
    };
    
    res.json({
        success: true,
        report
    });
});

// 헬퍼 함수들

function generateMonthSchedule(year, month, userId) {
    const daysInMonth = new Date(year, month, 0).getDate();
    const days = {};
    
    for (let day = 1; day <= daysInMonth; day++) {
        const date = new Date(year, month - 1, day);
        const isWeekend = date.getDay() === 0 || date.getDay() === 6;
        
        days[day] = {
            date: day,
            isWorkDay: !isWeekend && Math.random() > 0.3, // 70% 확률로 근무일
            shift: isWeekend ? null : (Math.random() > 0.5 ? 'morning' : 'afternoon'),
            room: isWeekend ? null : `room${Math.floor(Math.random() * 4) + 1}`,
            notes: ''
        };
    }
    
    return days;
}

// 에러 핸들링 미들웨어
app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(500).json({
        success: false,
        error: '서버 오류가 발생했습니다.'
    });
});

// 404 핸들링
app.use('*', (req, res) => {
    if (req.originalUrl.startsWith('/api/')) {
        res.status(404).json({
            success: false,
            error: 'API를 찾을 수 없습니다.'
        });
    } else {
        res.sendFile(path.join(__dirname, '..', 'index.html'));
    }
});

// 서버 시작
app.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 GC 내시경실 관리시스템이 http://localhost:${PORT}에서 실행 중입니다.`);
    console.log(`📝 테스트 계정:`);
    console.log(`   - 일반 사용자: 001 / user123`);
    console.log(`   - 관리자: admin / admin123`);
});

module.exports = app;