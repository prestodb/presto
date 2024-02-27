import React from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';
import useBaseUrl from "@docusaurus/useBaseUrl";



export default function VeloxConBanner() {
  return (
    <section className={styles.banner}>
      <div className="container">
        <div className="row">
          <div className="col col--6">
            <h2>Join us at VeloxCon</h2>
            <p>April 3–4, 2024 | San Jose, CA</p>
          </div>
          <div className="col col--6">
            <a className="button button--info button--lg" href="https://veloxcon.io/" target="_blank">Register Now</a>
          </div>
        </div>
      </div>
    </section>
  );
}
