/**
 * Global type overrides for browser environment
 * This ensures browser APIs are used instead of Node.js types
 */

// Override setTimeout/setInterval to return number (browser) instead of NodeJS.Timeout
declare function setTimeout(handler: TimerHandler, timeout?: number, ...arguments: any[]): number;
declare function clearTimeout(handle?: number): void;
declare function setInterval(handler: TimerHandler, timeout?: number, ...arguments: any[]): number;
declare function clearInterval(handle?: number): void;
declare function requestAnimationFrame(callback: FrameRequestCallback): number;
declare function cancelAnimationFrame(handle: number): void;
