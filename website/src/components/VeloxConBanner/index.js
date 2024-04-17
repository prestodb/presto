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
            <h2>See the VeloxCon 2024 Recordings</h2>
          </div>
          <div className="col col--3">
            <a className="button button--info button--lg" href="https://www.youtube.com/playlist?list=PLJvBe8nQAEsEBSoUY0lRFVZr2_YeHYkUR" target="_blank">Watch</a>
          </div>
        </div>
      </div>
    </section>
  );
}
