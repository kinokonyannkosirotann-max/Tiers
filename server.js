const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

// 待機キュー: { category: [ {socketId, user, isHighTier}, ... ] }
const queues = {};
const activeRooms = {}; // タイマー管理用

io.on('connection', (socket) => {
    console.log('User connected:', socket.id);

    // プレイヤーがキューに並ぶ
    socket.on('joinQueue', (data) => {
        const { user, category, isHighTier } = data;
        if (!queues[category]) queues[category] = [];
        
        // 10人制限
        if (queues[category].length >= 10) {
            return socket.emit('queueError', '現在キューが満員(10人)です。');
        }

        // LT3以上の場合は配列の先頭（優先）に挿入、そうでない場合は末尾
        const queueItem = { socketId: socket.id, user };
        if (isHighTier) {
            queues[category].unshift(queueItem);
        } else {
            queues[category].push(queueItem);
        }
        
        console.log(`${user.name} joined ${category} queue.`);
    });

    // テスターが対象を引き抜いてテスト開始
    socket.on('testerReady', (category) => {
        if (!queues[category] || queues[category].length === 0) {
            return socket.emit('chatEnded', '現在待機中のプレイヤーはいません。');
        }

        // 先頭のプレイヤー（優先順）をポップ
        const target = queues[category].shift();
        const roomId = `room_${socket.id}_${target.socketId}`;
        
        // 両者をルームに入れる
        socket.join(roomId);
        io.sockets.sockets.get(target.socketId)?.join(roomId);

        // 10分 (600,000ms) のタイマー設定
        const timeout = setTimeout(() => {
            io.to(roomId).emit('chatEnded', '10分経過したためチャットを強制終了しました。');
            io.in(roomId).socketsLeave(roomId);
            delete activeRooms[roomId];
        }, 600000);

        activeRooms[roomId] = { timeout };
        io.to(roomId).emit('testStarted', { roomId, targetUser: target.user });
    });

    // メッセージの送受信
    socket.on('sendMessage', (data) => {
        io.to(data.roomId).emit('receiveMessage', { 
            sender: data.sender, 
            message: data.message 
        });
    });

    // テスターによる強制終了・スキップ
    socket.on('endTest', (roomId) => {
        if (activeRooms[roomId]) {
            clearTimeout(activeRooms[roomId].timeout);
            delete activeRooms[roomId];
        }
        io.to(roomId).emit('chatEnded', 'テスターによりテストが終了されました。');
        io.in(roomId).socketsLeave(roomId);
    });

    socket.on('disconnect', () => {
        // 切断時は全キューから削除
        for (let cat in queues) {
            queues[cat] = queues[cat].filter(q => q.socketId !== socket.id);
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Realtime Server running on port ${PORT}`));
