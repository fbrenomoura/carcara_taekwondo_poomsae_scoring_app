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
    let devId = sessionStorage.getItem('poomsae_device_id');
    if (!devId) {
        devId = 'dev_' + Math.random().toString(36).substr(2, 9) + '_' + Date.now();
        sessionStorage.setItem('poomsae_device_id', devId);
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
                roundActive: false,
                penalties: 0,
                timerStart: null,
                timerPausedAt: null,
                consolidated: false,
                roundId: Date.now()
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

    // 2. Conectar como Juiz (Alocação Automática de Vaga)
    async joinAsJudge(code) {
        const sessionRef = doc(db, 'poomsae-sessions', code);
        const snap = await getDoc(sessionRef);

        if (!snap.exists()) {
            throw new Error("Sessão não encontrada com este código.");
        }

        const data = snap.data();
        const numJudges = data.numJudges || 3;
        const now = Date.now();

        let assignedJudgeNum = null;

        // 1º Verificar se este dispositivo já estava em uma vaga (Reconexão)
        for (let i = 1; i <= numJudges; i++) {
            const j = data.judges ? data.judges[`judge${i}`] : null;
            if (j && j.deviceId === this.deviceId) {
                assignedJudgeNum = i;
                break;
            }
        }

        // 2º Se não for reconexão, encontrar a primeira vaga vaga/disponível ou offline (timeout > 8s)
        if (!assignedJudgeNum) {
            for (let i = 1; i <= numJudges; i++) {
                const j = data.judges ? data.judges[`judge${i}`] : null;
                const isExpired = !j || (now - (j.lastSeen || 0)) > 8000;
                const isVacant = !j || j.status === 'vacant' || j.status === 'offline' || isExpired;
                
                if (isVacant) {
                    assignedJudgeNum = i;
                    break;
                }
            }
        }

        if (!assignedJudgeNum) {
            throw new Error(`Todas as ${numJudges} vagas de juízes para esta sessão já estão preenchidas.`);
        }

        const judgeKey = `judge${assignedJudgeNum}`;
        const updatePayload = {};
        updatePayload[`judges.${judgeKey}.deviceId`] = this.deviceId;
        updatePayload[`judges.${judgeKey}.lastSeen`] = now;
        updatePayload[`judges.${judgeKey}.status`] = 'online';

        // Verificar se com a entrada deste juiz todos estão conectados
        let onlineCount = 1; // Este juiz
        for (let i = 1; i <= numJudges; i++) {
            if (i === assignedJudgeNum) continue;
            const j = data.judges ? data.judges[`judge${i}`] : null;
            if (j && j.status === 'online' && (now - (j.lastSeen || 0)) <= 8000) {
                onlineCount++;
            }
        }

        if (onlineCount >= numJudges) {
            updatePayload['status'] = 'ready';
        }

        await updateDoc(sessionRef, updatePayload);

        this.sessionCode = code;
        this.role = 'judge';
        this.judgeNumber = assignedJudgeNum;

        this.listenToSession(code);
        this.startJudgeHeartbeat();
        return assignedJudgeNum;
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
    async sendJudgeScore(scores, isFinal = false) {
        if (this.role !== 'judge' || !this.sessionCode || !this.judgeNumber) return;
        
        // Evita escritas duplicadas se a nota não mudou e não é envio final
        const scoreStr = JSON.stringify({scores, isFinal});
        if (!isFinal && this.lastSentScores === scoreStr) return;

        const sessionRef = doc(db, 'poomsae-sessions', this.sessionCode);
        const judgeKey = `judge${this.judgeNumber}`;
        
        const updatePayload = {};
        updatePayload[`judges.${judgeKey}.scores`] = {
            ...scores,
            submitted: isFinal
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
            "sharedState.penalties": 0,
            "sharedState.timerStart": null,
            "sharedState.timerPausedAt": null,
            "sharedState.consolidated": false,
            "sharedState.roundId": Date.now(),
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

    // Master Calcular Nota Final Oficial (Com descarte independente de Maior/Menor para 5 ou 7 juízes)
    calculateFinalScore(judgesData, numJudges, penalties = 0) {
        const activeScores = [];
        Object.keys(judgesData).forEach(key => {
            const j = judgesData[key];
            if (j.scores && j.scores.submitted) {
                const accVal = j.scores.acc / 10;
                const presVal = (j.scores.speed + j.scores.rhythm + j.scores.energy) / 10;
                activeScores.push({ acc: accVal, pres: presVal });
            }
        });

        if (activeScores.length === 0) return { acc: "4.00", pres: "6.00", total: "10.00" };

        let accSum = 0;
        let presSum = 0;
        let count = activeScores.length;

        // Regra de descarte independente para 5 ou 7 juízes
        if ((numJudges === 5 || numJudges === 7) && activeScores.length >= 3) {
            const accList = activeScores.map(s => s.acc).sort((a, b) => a - b);
            const presList = activeScores.map(s => s.pres).sort((a, b) => a - b);
            
            // Remover menor (primeiro) e maior (último)
            const trimmedAcc = accList.slice(1, accList.length - 1);
            const trimmedPres = presList.slice(1, presList.length - 1);
            
            count = trimmedAcc.length;
            
            accSum = trimmedAcc.reduce((sum, val) => sum + val, 0);
            presSum = trimmedPres.reduce((sum, val) => sum + val, 0);
        } else {
            accSum = activeScores.reduce((sum, val) => sum + val.acc, 0);
            presSum = activeScores.reduce((sum, val) => sum + val.pres, 0);
        }

        const avgAcc = accSum / count;
        const avgPres = presSum / count;
        let totalScore = (avgAcc + avgPres) - (penalties * 0.3);
        if (totalScore < 0) totalScore = 0;

        return {
            acc: avgAcc.toFixed(2),
            pres: avgPres.toFixed(2),
            total: totalScore.toFixed(2)
        };
    }

    // Penalties and Timer (Master only)
    async addPenalty(amount = 1) {
        if (this.role !== 'master' || !this.sessionCode || !this.sessionData) return;
        const currentPenalties = this.sessionData.sharedState.penalties || 0;
        const sessionRef = doc(db, 'poomsae-sessions', this.sessionCode);
        await updateDoc(sessionRef, { "sharedState.penalties": currentPenalties + amount });
    }

    async startTimer() {
        if (this.role !== 'master' || !this.sessionCode || !this.sessionData) return;
        const sessionRef = doc(db, 'poomsae-sessions', this.sessionCode);
        let start = Date.now();
        // If resuming from pause, backdate start time to simulate elapsed time
        if (this.sessionData.sharedState.timerPausedAt && this.sessionData.sharedState.timerStart) {
            const elapsedBeforePause = this.sessionData.sharedState.timerPausedAt - this.sessionData.sharedState.timerStart;
            start = Date.now() - elapsedBeforePause;
        }
        await updateDoc(sessionRef, { 
            "sharedState.timerStart": start,
            "sharedState.timerPausedAt": null 
        });
    }

    async pauseTimer() {
        if (this.role !== 'master' || !this.sessionCode) return;
        const sessionRef = doc(db, 'poomsae-sessions', this.sessionCode);
        await updateDoc(sessionRef, { "sharedState.timerPausedAt": Date.now() });
    }

    async resetTimer() {
        if (this.role !== 'master' || !this.sessionCode) return;
        const sessionRef = doc(db, 'poomsae-sessions', this.sessionCode);
        await updateDoc(sessionRef, { 
            "sharedState.timerStart": null,
            "sharedState.timerPausedAt": null 
        });
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
