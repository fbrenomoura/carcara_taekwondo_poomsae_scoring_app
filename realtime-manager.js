// Realtime Manager - Carcara Poomsae Scoring System
// Optimizado para Free Tier (Mínimas leituras/escritas no Firestore)

import { 
    db, 
    doc, 
    setDoc, 
    getDoc, 
    updateDoc, 
    onSnapshot, 
    serverTimestamp 
} from './firebase-config.js';

// Gerador de ID Único local do dispositivo (evita auth desnecessária)
function getDeviceId() {
    let devId = localStorage.getItem('poomsae_device_id');
    if (!devId) {
        devId = 'dev_' + Math.random().toString(36).substr(2, 9) + '_' + Date.now();
        localStorage.setItem('poomsae_device_id', devId);
    }
    return devId;
}

export class RealtimeManager {
    constructor() {
        this.sessionCode = null;
        this.role = null; // 'master', 'judge', 'spectator'
        this.judgeNumber = null; // 1 to 7
        this.unsubscribe = null;
        this.heartbeatTimer = null;
        this.deviceId = getDeviceId();
        this.sessionData = null;
        this.onStateChangeCallback = null;
        this.lastSentScores = null;
    }

    // Gerar código aleatório de 6 dígitos
    generateCode() {
        return Math.floor(100000 + Math.random() * 900000).toString();
    }

    // 1. Criar Sessão (Mesa Central / Master)
    async createSession(numJudges = 3) {
        const code = this.generateCode();
        const sessionRef = doc(db, 'poomsae-sessions', code);
        
        const initialJudges = {};
        for (let i = 1; i <= numJudges; i++) {
            initialJudges[`judge${i}`] = {
                deviceId: null,
                lastSeen: 0,
                status: 'vacant', // 'vacant', 'online', 'offline'
                scores: { acc: 40, speed: 20, rhythm: 20, energy: 20, submitted: false }
            };
        }

        const sessionPayload = {
            code: code,
            numJudges: parseInt(numJudges),
            status: 'waiting', // waiting, scoring, finished
            masterDeviceId: this.deviceId,
            masterLastSeen: Date.now(),
            sharedState: {
                accDisplay: "4.0",
                presDisplay: "6.0",
                totalDisplay: "10.0",
                roundActive: false
            },
            judges: initialJudges,
            updatedAt: Date.now()
        };

        await setDoc(sessionRef, sessionPayload);
        this.sessionCode = code;
        this.role = 'master';
        this.listenToSession(code);
        this.startMasterHeartbeat();
        return code;
    }

    // 2. Conectar como Juiz
    async joinAsJudge(code, selectedJudgeNum) {
        const sessionRef = doc(db, 'poomsae-sessions', code);
        const snap = await getDoc(sessionRef);

        if (!snap.exists()) {
            throw new Error("Sessão não encontrada com este código.");
        }

        const data = snap.data();
        const judgeKey = `judge${selectedJudgeNum}`;
        const targetJudge = data.judges ? data.judges[judgeKey] : null;

        if (!targetJudge) {
            throw new Error(`Juiz ${selectedJudgeNum} não está disponível para esta sessão de ${data.numJudges} juízes.`);
        }

        const now = Date.now();
        // Verificar se vaga está livre ou se é a reconexão do mesmo dispositivo ou se deu timeout (8s)
        const isExpired = (now - (targetJudge.lastSeen || 0)) > 8000;
        const isSameDevice = targetJudge.deviceId === this.deviceId;

        if (targetJudge.status === 'online' && !isExpired && !isSameDevice) {
            throw new Error(`A vaga de Juiz ${selectedJudgeNum} já está ocupada por outro dispositivo.`);
        }

        // Assumir Vaga
        const updatePayload = {};
        updatePayload[`judges.${judgeKey}.deviceId`] = this.deviceId;
        updatePayload[`judges.${judgeKey}.lastSeen`] = now;
        updatePayload[`judges.${judgeKey}.status`] = 'online';

        await updateDoc(sessionRef, updatePayload);

        this.sessionCode = code;
        this.role = 'judge';
        this.judgeNumber = selectedJudgeNum;

        this.listenToSession(code);
        this.startJudgeHeartbeat();
        return true;
    }

    // 3. Conectar como Espectador (Telão)
    async joinAsSpectator(code) {
        const sessionRef = doc(db, 'poomsae-sessions', code);
        const snap = await getDoc(sessionRef);

        if (!snap.exists()) {
            throw new Error("Sessão não encontrada com este código.");
        }

        this.sessionCode = code;
        this.role = 'spectator';
        this.listenToSession(code);
        return snap.data();
    }

    // Escutar atualizações do Firestore em tempo real (Modo ultra otimizado)
    listenToSession(code) {
        if (this.unsubscribe) this.unsubscribe();
        const sessionRef = doc(db, 'poomsae-sessions', code);

        this.unsubscribe = onSnapshot(sessionRef, (docSnap) => {
            if (docSnap.exists()) {
                this.sessionData = docSnap.data();
                if (this.role === 'master') {
                    this.checkJudgeTimeouts();
                }
                if (this.onStateChangeCallback) {
                    this.onStateChangeCallback(this.sessionData);
                }
            }
        }, (error) => {
            console.error("Erro no listener Firestore:", error);
        });
    }

    // Heartbeat do Juiz - Otimizado (Economia de gravações no Firestore)
    startJudgeHeartbeat() {
        if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
        
        // Atualiza lastSeen a cada 3 segundos para economizar writes no Free Tier (mantendo tolerância de 8s)
        this.heartbeatTimer = setInterval(async () => {
            if (!this.sessionCode || this.role !== 'judge') return;
            try {
                const sessionRef = doc(db, 'poomsae-sessions', this.sessionCode);
                const judgeKey = `judge${this.judgeNumber}`;
                const updatePayload = {};
                updatePayload[`judges.${judgeKey}.lastSeen`] = Date.now();
                updatePayload[`judges.${judgeKey}.status`] = 'online';
                await updateDoc(sessionRef, updatePayload);
            } catch (err) {
                console.warn("Erro no Heartbeat:", err);
            }
        }, 3000);
    }

    // Heartbeat do Master
    startMasterHeartbeat() {
        if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
        this.heartbeatTimer = setInterval(async () => {
            if (!this.sessionCode || this.role !== 'master') return;
            try {
                const sessionRef = doc(db, 'poomsae-sessions', this.sessionCode);
                await updateDoc(sessionRef, { masterLastSeen: Date.now() });
            } catch (err) {
                console.warn("Erro no Heartbeat do Master:", err);
            }
        }, 4000);
    }

    // Checar desconexão de juízes (Timeout 8s) executado pelo Master
    async checkJudgeTimeouts() {
        if (!this.sessionData || !this.sessionData.judges) return;
        const now = Date.now();
        let needsUpdate = false;
        const updatePayload = {};

        Object.keys(this.sessionData.judges).forEach(key => {
            const judge = this.sessionData.judges[key];
            if (judge.status === 'online' && (now - (judge.lastSeen || 0)) > 8000) {
                updatePayload[`judges.${key}.status`] = 'offline';
                needsUpdate = true;
            }
        });

        if (needsUpdate && this.sessionCode) {
            const sessionRef = doc(db, 'poomsae-sessions', this.sessionCode);
            await updateDoc(sessionRef, updatePayload);
        }
    }

    // Juiz Enviar Nota
    async sendJudgeScore(scores) {
        if (this.role !== 'judge' || !this.sessionCode || !this.judgeNumber) return;
        
        // Evita escritas duplicadas se a nota não mudou
        const scoreStr = JSON.stringify(scores);
        if (this.lastSentScores === scoreStr) return;

        const sessionRef = doc(db, 'poomsae-sessions', this.sessionCode);
        const judgeKey = `judge${this.judgeNumber}`;
        
        const updatePayload = {};
        updatePayload[`judges.${judgeKey}.scores`] = {
            ...scores,
            submitted: true
        };
        updatePayload[`judges.${judgeKey}.lastSeen`] = Date.now();

        await updateDoc(sessionRef, updatePayload);
        this.lastSentScores = scoreStr;
    }

    // Master Reiniciar Rodada
    async resetRound() {
        if (this.role !== 'master' || !this.sessionCode) return;
        const sessionRef = doc(db, 'poomsae-sessions', this.sessionCode);

        const updatePayload = {
            "sharedState.roundActive": true,
            "sharedState.accDisplay": "4.0",
            "sharedState.presDisplay": "6.0",
            "sharedState.totalDisplay": "10.0",
            status: 'scoring'
        };

        // Resetar envio dos juízes
        if (this.sessionData && this.sessionData.judges) {
            Object.keys(this.sessionData.judges).forEach(key => {
                updatePayload[`judges.${key}.scores`] = { acc: 40, speed: 20, rhythm: 20, energy: 20, submitted: false };
            });
        }

        await updateDoc(sessionRef, updatePayload);
    }

    // Master Calcular Nota Final Oficial (Com descarte de Maior/Menor para 5 ou 7 juízes)
    calculateFinalScore(judgesData, numJudges) {
        const activeScores = [];
        Object.keys(judgesData).forEach(key => {
            const j = judgesData[key];
            if (j.scores) {
                const accVal = j.scores.acc / 10;
                const presVal = (j.scores.speed + j.scores.rhythm + j.scores.energy) / 10;
                const total = accVal + presVal;
                activeScores.push({ acc: accVal, pres: presVal, total: total });
            }
        });

        if (activeScores.length === 0) return { acc: "0.0", pres: "0.0", total: "0.0" };

        let totalAccSum = 0;
        let totalPresSum = 0;
        let totalSum = 0;
        let count = activeScores.length;

        // Regra de descarte para 5 ou 7 juízes
        if ((numJudges === 5 || numJudges === 7) && activeScores.length >= 3) {
            // Ordenar por pontuação total
            activeScores.sort((a, b) => a.total - b.total);
            // Remover menor nota (primeira) e maior nota (última)
            const trimmedScores = activeScores.slice(1, activeScores.length - 1);
            count = trimmedScores.length;
            
            trimmedScores.forEach(s => {
                totalAccSum += s.acc;
                totalPresSum += s.pres;
                totalSum += s.total;
            });
        } else {
            activeScores.forEach(s => {
                totalAccSum += s.acc;
                totalPresSum += s.pres;
                totalSum += s.total;
            });
        }

        return {
            acc: (totalAccSum / count).toFixed(2),
            pres: (totalPresSum / count).toFixed(2),
            total: (totalSum / count).toFixed(2)
        };
    }

    // Sair / Desconectar
    async leaveSession() {
        if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
        if (this.unsubscribe) this.unsubscribe();

        if (this.sessionCode && this.role === 'judge' && this.judgeNumber) {
            try {
                const sessionRef = doc(db, 'poomsae-sessions', this.sessionCode);
                const judgeKey = `judge${this.judgeNumber}`;
                const updatePayload = {};
                updatePayload[`judges.${judgeKey}.status`] = 'vacant';
                updatePayload[`judges.${judgeKey}.deviceId`] = null;
                await updateDoc(sessionRef, updatePayload);
            } catch (e) {
                console.warn("Erro ao liberar vaga ao sair:", e);
            }
        }

        this.sessionCode = null;
        this.role = null;
        this.judgeNumber = null;
        this.sessionData = null;
    }
}

export const realtimeManager = new RealtimeManager();
