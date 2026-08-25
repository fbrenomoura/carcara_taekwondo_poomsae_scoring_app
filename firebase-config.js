// Firebase Initialization for Poomsae Scoring App
// Project ID: poomsae-c1ba0

import { initializeApp } from "https://www.gstatic.com/firebasejs/10.8.0/firebase-app.js";
import { 
    getFirestore, 
    doc, 
    setDoc, 
    getDoc, 
    updateDoc, 
    onSnapshot, 
    deleteDoc, 
    serverTimestamp 
} from "https://www.gstatic.com/firebasejs/10.8.0/firebase-firestore.js";

const firebaseConfig = {
    projectId: "poomsae-c1ba0",
    authDomain: "poomsae-c1ba0.firebaseapp.com",
    storageBucket: "poomsae-c1ba0.appspot.com"
};

// Initialize Firebase App
const app = initializeApp(firebaseConfig);
const db = getFirestore(app);

export { 
    db, 
    doc, 
    setDoc, 
    getDoc, 
    updateDoc, 
    onSnapshot, 
    deleteDoc, 
    serverTimestamp 
};
