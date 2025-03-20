import React from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';
import useBaseUrl from "@docusaurus/useBaseUrl";



export default function VeloxConBanner() {
  return (
    <section className={styles.banner}>
      <div className="container">
        <div className="row">
          <div className="col col--9">
            <h2>Join us at VeloxCon 2025, April 15-16 at Meta HQ</h2>
          </div>
          <div className="col col--3">
            <a className="button button--info button--lg" href="https://veloxcon.io/" target="_blank" rel="noopener noreferrer">Free Registration</a>
          </div>
        </div>
      </div>
    </section>
  );
}
